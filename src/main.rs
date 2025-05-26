use clap::Parser;
use futures::sink::SinkExt;
use three_d::*;
use tokio::{select, spawn};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut},
    codec::{Decoder, Encoder, Framed},
};

#[derive(Parser)]
struct Args {
    /// Serial port to open
    #[arg(short, long)]
    port: Option<String>,
    /// List known serial ports
    #[arg(short, long)]
    list: bool,
}

fn describe_port_type(port_type: &tokio_serial::SerialPortType) -> &'static str {
    match port_type {
        tokio_serial::SerialPortType::UsbPort(_) => "USB",
        tokio_serial::SerialPortType::PciPort => "PCI",
        tokio_serial::SerialPortType::BluetoothPort => "BT",
        tokio_serial::SerialPortType::Unknown => "UNKNOWN",
    }
}

pub const HEADER_SIZE: usize = 16;
pub const MAX_FRAME_SIZE: usize = 100 * 100;
#[derive(Clone, Copy)]
struct A010Frame {
    pub full_size: usize,
    pub header: [u8; HEADER_SIZE],
    pub frame_size: usize,
    pub data: [u8; MAX_FRAME_SIZE],
}

impl Default for A010Frame {
    fn default() -> Self {
        Self {
            full_size: MAX_FRAME_SIZE + HEADER_SIZE,
            header: [0; HEADER_SIZE],
            frame_size: MAX_FRAME_SIZE,
            data: [0xFF; MAX_FRAME_SIZE],
        }
    }
}

type FrameSender = tokio::sync::watch::Sender<A010Frame>;
type FrameReceiver = tokio::sync::watch::Receiver<A010Frame>;

enum A010CodecStatus {
    Cmd,
    FrameStart,
    FrameSizeMsb,
    FrameSizeLsb,
    FrameHeader,
    FrameData,
    FrameSum,
    FrameStop,
}

const CMD_BUF_SIZE: usize = 128;
const FRAME_BUF_SIZE: usize = MAX_FRAME_SIZE + 32;
struct A010Codec {
    status: A010CodecStatus,
    cmd_pointer: usize,
    frame_pointer: usize,
    cmd_buf: [u8; CMD_BUF_SIZE],
    frame: A010Frame,
    sender: FrameSender,
}

type A010CodecFramed = Framed<SerialStream, A010Codec>;

impl A010Codec {
    pub fn new() -> (Self, FrameReceiver) {
        let (sender, receiver) = tokio::sync::watch::channel(A010Frame::default());
        (
            Self {
                status: A010CodecStatus::Cmd,
                cmd_pointer: 0,
                frame_pointer: 0,
                cmd_buf: [0; CMD_BUF_SIZE],
                frame: A010Frame::default(),
                sender,
            },
            receiver,
        )
    }

    fn reset_frame(&mut self) {
        self.frame_pointer = 0;
        self.frame.full_size = 0;
        self.frame.frame_size = 0;
    }

    fn handle_command(&mut self) -> Option<String> {
        if self.cmd_pointer > 0 {
            let cmd = String::from_utf8_lossy(&self.cmd_buf[..self.cmd_pointer]);
            self.cmd_pointer = 0;
            return Some(cmd.to_string());
        }
        None
    }

    fn handle_byte(&mut self, byte: u8) -> Option<String> {
        match self.status {
            A010CodecStatus::Cmd => {
                if byte == 0x00 {
                    self.status = A010CodecStatus::FrameStart;
                } else if byte == b'\r' || byte == b'\n' {
                    // Handle command
                    if let Some(cmd) = self.handle_command() {
                        return Some(cmd);
                    }
                } else {
                    if self.cmd_pointer >= CMD_BUF_SIZE {
                        // Handle partial command
                        if let Some(cmd) = self.handle_command() {
                            return Some(cmd);
                        }
                    }
                    self.cmd_buf[self.cmd_pointer] = byte;
                    self.cmd_pointer += 1;
                }
            }
            A010CodecStatus::FrameStart => {
                if byte == 0xFF {
                    self.status = A010CodecStatus::FrameSizeLsb;
                } else {
                    self.reset_frame();
                    self.status = A010CodecStatus::Cmd;
                }
            }
            A010CodecStatus::FrameSizeLsb => {
                self.frame.full_size |= byte as usize;
                if self.frame.full_size > FRAME_BUF_SIZE {
                    self.reset_frame();
                }
                self.frame_pointer = 0;
                self.status = A010CodecStatus::FrameSizeMsb;
            }
            A010CodecStatus::FrameSizeMsb => {
                self.frame.full_size |= (byte as usize) << 8;
                self.frame.frame_size = self.frame.full_size - HEADER_SIZE;
                self.status = A010CodecStatus::FrameHeader;
                self.frame_pointer = 0;
            }
            A010CodecStatus::FrameHeader => {
                self.frame.header[self.frame_pointer] = byte;
                self.frame_pointer += 1;
                if self.frame_pointer >= HEADER_SIZE {
                    self.frame_pointer = 0;
                    self.status = A010CodecStatus::FrameData;
                }
            }
            A010CodecStatus::FrameData => {
                self.frame.data[self.frame_pointer] = byte;
                self.frame_pointer += 1;
                if self.frame_pointer >= self.frame.frame_size as usize {
                    self.status = A010CodecStatus::FrameSum;
                }
            }
            A010CodecStatus::FrameSum => {
                let mut sum = 0;
                for i in 0..HEADER_SIZE {
                    sum += self.frame.header[i];
                }
                for i in 0..self.frame_pointer {
                    sum += self.frame.data[i];
                }
                sum += self.frame.full_size as u8;
                sum += (self.frame.full_size >> 8) as u8;
                sum -= 1;
                // Handle checksum
                if sum != byte {
                    // Handle checksum error
                    println!("Checksum error: expected {}, got {}", sum, byte);
                }
                self.status = A010CodecStatus::FrameStop;
            }
            A010CodecStatus::FrameStop => {
                self.sender.send(self.frame).ok();
                self.reset_frame();
                self.status = A010CodecStatus::Cmd;
            }
        }

        None
    }
}

fn write_at_get_arg(name: &str, dst: &mut tokio_util::bytes::BytesMut) {
    dst.extend_from_slice(b"AT+");
    dst.extend_from_slice(name.as_bytes());
    dst.extend_from_slice(b"?\r\n");
}

fn write_at_set_arg(name: &str, value: u8, dst: &mut tokio_util::bytes::BytesMut) {
    let mut value = value;
    dst.extend_from_slice(b"AT+");
    dst.extend_from_slice(name.as_bytes());
    dst.put_u8('=' as u8);
    if value >= 100 {
        dst.put_u8((value / 100) as u8 + b'0');
        value %= 100;
    }
    if value >= 10 {
        dst.put_u8((value / 10) as u8 + b'0');
        value %= 10;
    }
    dst.put_u8(value + b'0');
    dst.extend_from_slice(b"\r\n");
}

fn decode_at_arg(name: &str, from: &[u8]) -> Option<u8> {
    from.strip_prefix(b"+")
        .and_then(|buf| buf.strip_prefix(name.as_bytes()))
        .and_then(|buf| buf.strip_prefix(b"="))
        .map(|buf| buf.trim_ascii_start())
        .and_then(|buf| {
            if buf.len() < 1 || buf[0] < b'0' || buf[0] > b'9' {
                return None;
            }
            let mut value = 0;
            for &byte in buf.iter() {
                if byte < b'0' || byte > b'9' {
                    return None;
                }
                value = value * 10 + (byte - b'0') as u8;
            }
            Some(value)
        })
}

fn numeric_value_name_0_20(value: u8) -> &'static str {
    match value {
        0 => "0",
        1 => "1",
        2 => "2",
        3 => "3",
        4 => "4",
        5 => "5",
        6 => "6",
        7 => "7",
        8 => "8",
        9 => "9",
        10 => "10",
        11 => "11",
        12 => "12",
        13 => "13",
        14 => "14",
        15 => "15",
        16 => "16",
        17 => "17",
        18 => "18",
        19 => "19",
        20 => "20",
        _ => panic!("value not in [0 ..= 20] range"),
    }
}

pub trait AtArg: Sized {
    const NAME: &'static str;
    const MIN: u8;
    const MAX: u8;
    const DEFAULT: u8 = Self::MIN;
    const EXCLUDE_VALUES: &'static [u8] = &[];

    fn value(&self) -> u8;
    fn new_unchecked(value: u8) -> Self;

    fn value_name(&self) -> &'static str {
        numeric_value_name_0_20(self.value())
    }

    fn new(value: u8) -> Option<Self> {
        Self::check_value(value).map(Self::new_unchecked)
    }

    fn default() -> Self {
        Self::new_unchecked(Self::DEFAULT)
    }

    fn encode_get(dst: &mut tokio_util::bytes::BytesMut) {
        write_at_get_arg(Self::NAME, dst);
    }

    fn encode_set(&self, dst: &mut tokio_util::bytes::BytesMut) {
        write_at_set_arg(Self::NAME, self.value(), dst);
    }

    fn decode(from: &[u8]) -> Option<Self> {
        decode_at_arg(Self::NAME, from).and_then(Self::new)
    }

    fn check_value(value: u8) -> Option<u8> {
        if value < Self::MIN || value > Self::MAX || Self::EXCLUDE_VALUES.contains(&value) {
            None
        } else {
            Some(value)
        }
    }

    fn allowed_values() -> Vec<Self> {
        (Self::MIN..=Self::MAX).filter_map(Self::new).collect()
    }
}

#[test]
fn test_at_arg() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestArg(u8);
    impl AtArg for TestArg {
        const NAME: &'static str = "TEST";
        const MIN: u8 = 0;
        const MAX: u8 = 10;
        const EXCLUDE_VALUES: &'static [u8] = &[3, 5, 7];

        fn value(&self) -> u8 {
            self.0
        }

        fn new_unchecked(value: u8) -> Self {
            Self(value)
        }
    }

    let mut buf = tokio_util::bytes::BytesMut::with_capacity(32);

    let arg = TestArg::new(4).unwrap();
    arg.encode_set(&mut buf);
    assert_eq!(buf.as_ref(), b"AT+TEST=4\r\n");
    buf.clear();
    TestArg::encode_get(&mut buf);
    assert_eq!(buf.as_ref(), b"AT+TEST?\r\n");

    let decoded = TestArg::decode(b"+TEST=4").unwrap();
    assert_eq!(decoded.value(), 4);
    assert_eq!(decoded.value_name(), "4");
    assert!(TestArg::new(3).is_none());
    assert!(TestArg::new(5).is_none());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BINN(u8);
impl AtArg for BINN {
    const NAME: &'static str = "BINN";
    const MIN: u8 = 1;
    const MAX: u8 = 4;
    const EXCLUDE_VALUES: &'static [u8] = &[3];

    fn value(&self) -> u8 {
        self.0
    }

    fn new_unchecked(value: u8) -> Self {
        Self(value)
    }

    fn value_name(&self) -> &'static str {
        match self.value() {
            1 => "1x1",
            2 => "2x2",
            4 => "4x4",
            _ => panic!("value not in [1, 2, 4]"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DISP(u8);
impl AtArg for DISP {
    const NAME: &'static str = "DISP";
    const MIN: u8 = 0;
    const MAX: u8 = 7;

    fn value(&self) -> u8 {
        self.0
    }

    fn new_unchecked(value: u8) -> Self {
        Self(value)
    }

    fn value_name(&self) -> &'static str {
        match self.value() {
            0 => "OFF",
            1 => "LCD",
            2 => "USB",
            3 => "LCD,USB",
            4 => "UART",
            5 => "LCD,UART",
            6 => "USB,UART",
            7 => "LCD,USB,UART",
            _ => panic!("value not in [0..=7] range"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BAUD(u8);
impl AtArg for BAUD {
    const NAME: &'static str = "BAUD";
    const MIN: u8 = 0;
    const MAX: u8 = 8;
    const DEFAULT: u8 = 2;

    fn value(&self) -> u8 {
        self.0
    }

    fn new_unchecked(value: u8) -> Self {
        Self(value)
    }

    fn value_name(&self) -> &'static str {
        match self.value() {
            0 => "9600",
            1 => "57600",
            2 => "115200",
            3 => "230400",
            4 => "460800",
            5 => "921600",
            6 => "1000000",
            7 => "2000000",
            8 => "3000000",
            _ => panic!("value not in [0..=8] range"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UNIT(u8);
impl AtArg for UNIT {
    const NAME: &'static str = "UNIT";
    const MIN: u8 = 0;
    const MAX: u8 = 9;

    fn value(&self) -> u8 {
        self.0
    }

    fn new_unchecked(value: u8) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FPS(u8);
impl AtArg for FPS {
    const NAME: &'static str = "FPS";
    const MIN: u8 = 1;
    const MAX: u8 = 19;

    fn value(&self) -> u8 {
        self.0
    }

    fn new_unchecked(value: u8) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A010Arg {
    Binn(BINN),
    Disp(DISP),
    Baud(BAUD),
    Unit(UNIT),
    Fps(FPS),
}

impl A010Arg {
    fn encode_get_binn(dst: &mut tokio_util::bytes::BytesMut) {
        BINN::encode_get(dst)
    }
    fn encode_get_disp(dst: &mut tokio_util::bytes::BytesMut) {
        DISP::encode_get(dst)
    }
    fn encode_get_baud(dst: &mut tokio_util::bytes::BytesMut) {
        BAUD::encode_get(dst)
    }
    fn encode_get_unit(dst: &mut tokio_util::bytes::BytesMut) {
        UNIT::encode_get(dst)
    }
    fn encode_get_fps(dst: &mut tokio_util::bytes::BytesMut) {
        FPS::encode_get(dst)
    }

    fn encode_set(&self, dst: &mut tokio_util::bytes::BytesMut) {
        match self {
            A010Arg::Binn(binn) => binn.encode_set(dst),
            A010Arg::Disp(disp) => disp.encode_set(dst),
            A010Arg::Baud(baud) => baud.encode_set(dst),
            A010Arg::Unit(unit) => unit.encode_set(dst),
            A010Arg::Fps(fps) => fps.encode_set(dst),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A010Cmd {
    AT,
    GetBinn,
    GetDisp,
    GetBaud,
    GetUnit,
    GetFps,
    Set(A010Arg),
}

impl A010Cmd {
    pub fn as_arg(&self) -> Option<A010Arg> {
        match self {
            A010Cmd::Set(arg) => Some(*arg),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A010Rsp {
    OK,
    Binn(BINN),
    Disp(DISP),
    Baud(BAUD),
    Unit(UNIT),
    Fps(FPS),
}

impl A010Rsp {
    pub fn as_arg(&self) -> Option<A010Arg> {
        match self {
            A010Rsp::OK => None,
            A010Rsp::Binn(binn) => Some(A010Arg::Binn(*binn)),
            A010Rsp::Disp(disp) => Some(A010Arg::Disp(*disp)),
            A010Rsp::Baud(baud) => Some(A010Arg::Baud(*baud)),
            A010Rsp::Unit(unit) => Some(A010Arg::Unit(*unit)),
            A010Rsp::Fps(fps) => Some(A010Arg::Fps(*fps)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A010Config {
    pub binn: BINN,
    pub disp: DISP,
    pub baud: BAUD,
    pub unit: UNIT,
    pub fps: FPS,
}

impl Default for A010Config {
    fn default() -> Self {
        Self {
            binn: BINN::default(),
            disp: DISP::default(),
            baud: BAUD::default(),
            unit: UNIT::default(),
            fps: FPS::default(),
        }
    }
}

impl A010Config {
    pub fn apply(&mut self, arg: A010Arg) {
        match arg {
            A010Arg::Binn(binn) => self.binn = binn,
            A010Arg::Disp(disp) => self.disp = disp,
            A010Arg::Baud(baud) => self.baud = baud,
            A010Arg::Unit(unit) => self.unit = unit,
            A010Arg::Fps(fps) => self.fps = fps,
        }
    }
}

impl Decoder for A010Codec {
    type Item = A010Rsp;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        while src.remaining() > 0 {
            let byte = src.get_u8();
            if let Some(cmd) = self.handle_byte(byte) {
                println!("Decoding response string: {}", cmd);

                if cmd == "OK" {
                    return Ok(Some(A010Rsp::OK));
                } else if let Some(arg) = BINN::decode(cmd.as_bytes()) {
                    return Ok(Some(A010Rsp::Binn(arg)));
                } else if let Some(arg) = DISP::decode(cmd.as_bytes()) {
                    return Ok(Some(A010Rsp::Disp(arg)));
                } else if let Some(arg) = BAUD::decode(cmd.as_bytes()) {
                    return Ok(Some(A010Rsp::Baud(arg)));
                } else if let Some(arg) = UNIT::decode(cmd.as_bytes()) {
                    return Ok(Some(A010Rsp::Unit(arg)));
                } else if let Some(arg) = FPS::decode(cmd.as_bytes()) {
                    return Ok(Some(A010Rsp::Fps(arg)));
                } else {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
}

impl Encoder<A010Cmd> for A010Codec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: A010Cmd,
        dst: &mut tokio_util::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        println!("Encoding command: {:?}", item);

        match item {
            A010Cmd::AT => {
                dst.extend_from_slice(b"AT\r\n");
            }
            A010Cmd::GetBinn => A010Arg::encode_get_binn(dst),
            A010Cmd::GetDisp => A010Arg::encode_get_disp(dst),
            A010Cmd::GetBaud => A010Arg::encode_get_baud(dst),
            A010Cmd::GetUnit => A010Arg::encode_get_unit(dst),
            A010Cmd::GetFps => A010Arg::encode_get_fps(dst),
            A010Cmd::Set(arg) => arg.encode_set(dst),
        }
        Ok(())
    }
}

type AtCmdSender = tokio::sync::mpsc::Sender<A010Cmd>;
type AtCmdReceiver = tokio::sync::mpsc::Receiver<A010Cmd>;

type ConfigSender = tokio::sync::watch::Sender<A010Config>;
type ConfigReceiver = tokio::sync::watch::Receiver<A010Config>;

async fn handle_cmd(a010: &mut A010CodecFramed, cfg: &mut A010Config, cmd: A010Cmd) -> bool {
    let arg = cmd.as_arg();

    if let Err(err) = a010.send(cmd).await {
        println!("Error sending command {:?}: {}", cmd, err);
        return true;
    }

    loop {
        println!("Waiting for response to command {:?}", cmd);

        let rsp = match a010.next().await.transpose() {
            Ok(rsp) => match rsp {
                Some(rsp) => rsp,
                None => {
                    println!("response stream terminated");
                    return true;
                }
            },
            Err(err) => {
                println!("Error receiving response to command {:?}: {}", cmd, err);
                return true;
            }
        };

        println!("Received response: {:?}", rsp);
        let arg = arg.or(rsp.as_arg());
        if let Some(arg) = arg {
            cfg.apply(arg);
        }

        if rsp == A010Rsp::OK {
            println!("Command {:?} executed successfully.", cmd);
            return false;
        }
    }
}

async fn handle_a010_connection(
    a010: A010CodecFramed,
    commands: AtCmdReceiver,
    config: ConfigSender,
) {
    let mut a010 = a010;
    let mut commands = commands;

    let mut cfg = A010Config::default();

    println!("Requesting initial configuration...");
    println!("GET BINN");
    if handle_cmd(&mut a010, &mut cfg, A010Cmd::GetBinn).await {
        return;
    }
    println!("GET DISP");
    if handle_cmd(&mut a010, &mut cfg, A010Cmd::GetDisp).await {
        return;
    }
    println!("GET BAUD");
    if handle_cmd(&mut a010, &mut cfg, A010Cmd::GetBaud).await {
        return;
    }
    println!("GET UNIT");
    if handle_cmd(&mut a010, &mut cfg, A010Cmd::GetUnit).await {
        return;
    }
    println!("GET FPS");
    if handle_cmd(&mut a010, &mut cfg, A010Cmd::GetFps).await {
        return;
    }
    println!("Initial configuration requested.");

    loop {
        if let Err(_) = config.send(cfg) {
            println!("error sending config");
            return;
        }

        let command = select! {
            command = commands.recv() => {
                command
            }
            response = a010.next() => {
                let response = response.transpose().unwrap();
                if let Some(rsp) = response {
                    if let Some(arg) = rsp.as_arg() {
                        cfg.apply(arg);
                    }
                } else {
                    println!("response stream terminated");
                    return;
                }
                None
            }
        };

        if let Some(cmd) = command {
            if handle_cmd(&mut a010, &mut cfg, cmd).await {
                return;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.list {
        // List known serial ports
        let ports = tokio_serial::available_ports().unwrap();
        for port in ports {
            println!(
                "{} ({})",
                &port.port_name,
                describe_port_type(&port.port_type)
            );
        }
        return;
    }

    let port = if let Some(port) = &args.port {
        println!("serial port: {}", port);
        port
    } else {
        println!("No serial port specified");
        return;
    };

    let serial = tokio_serial::new(port, 115200)
        .data_bits(tokio_serial::DataBits::Eight)
        .stop_bits(tokio_serial::StopBits::One)
        .parity(tokio_serial::Parity::None)
        .open_native_async()
        .unwrap();

    let (codec, frame_receiver) = A010Codec::new();
    let a010: A010CodecFramed = Framed::new(serial, codec);

    let (cmd_sender, cmd_receiver) = tokio::sync::mpsc::channel(32);
    let (cfg_sender, cfg_receiver) = tokio::sync::watch::channel(A010Config::default());
    spawn(handle_a010_connection(a010, cmd_receiver, cfg_sender));
    main_window(frame_receiver, cmd_sender, cfg_receiver);
}

fn setup_frame_indices(indices: &mut Vec<u16>, side: usize) {
    indices.clear();
    for ix in 0..side - 1 {
        for iy in 0..side - 1 {
            let i = ix + iy * side;
            let i0 = i as u16;
            let i1 = (i + 1) as u16;
            let i2 = (i + side) as u16;
            let i3 = (i + side + 1) as u16;

            if i % 2 == 0 {
                indices.push(i0);
                indices.push(i1);
                indices.push(i2);
                indices.push(i1);
                indices.push(i3);
                indices.push(i2);
            } else {
                indices.push(i0);
                indices.push(i1);
                indices.push(i3);
                indices.push(i0);
                indices.push(i3);
                indices.push(i2);
            }
        }
    }
}

const FRAME_COLOR_EVEN: Srgba = Srgba {
    r: 192,
    g: 192,
    b: 192,
    a: 255,
};
const FRAME_COLOR_ODD: Srgba = Srgba {
    r: 64,
    g: 64,
    b: 64,
    a: 255,
};

fn setup_frame_colors(colors: &mut Vec<Srgba>, side: usize) {
    colors.clear();
    for ix in 0..side {
        for iy in 0..side {
            if (ix + iy) % 2 == 0 {
                colors.push(FRAME_COLOR_EVEN);
            } else {
                colors.push(FRAME_COLOR_ODD);
            }
        }
    }
}

const FRAME_APERTURE_W: f32 = 60.0;
const FRAME_APERTURE_H: f32 = 60.0;
const FRAME_DISTANCE_SCALE: f32 = 1.0;

const MAX_Z: f32 = 255.0;

fn setup_frame(frame: &A010Frame, mesh: &mut CpuMesh) -> bool {
    let positions = match &mut mesh.positions {
        Positions::F32(points) => points,
        _ => return false,
    };
    let indices = match &mut mesh.indices {
        Indices::U16(items) => items,
        _ => return false,
    };
    let colors = match &mut mesh.colors {
        Some(colors) => colors,
        None => return false,
    };
    let side = match frame.frame_size {
        10000 => 100,
        2500 => 50,
        625 => 25,
        _ => return false,
    };

    if positions.len() != side * side {
        setup_frame_indices(indices, side);
        setup_frame_colors(colors, side);
    }

    let dw = FRAME_APERTURE_W / side as f32;
    let dh = FRAME_APERTURE_H / side as f32;

    let ax0 = -FRAME_APERTURE_W / 2.0;
    let ay0 = -FRAME_APERTURE_H / 2.0;

    positions.clear();
    for ix in 0..side {
        for iy in 0..side {
            let i = ix + iy * side;
            let distance = frame.data[i] as f32 * FRAME_DISTANCE_SCALE;
            let z = MAX_Z - distance;
            let ax = ax0 + (ix as f32 * dw);
            let ay = ay0 + (iy as f32 * dh);
            let x = ax.to_radians().sin() * distance * FRAME_DISTANCE_SCALE;
            let y = ay.to_radians().sin() * distance * FRAME_DISTANCE_SCALE;
            positions.push(vec3(x, y, z));
        }
    }

    true
}

const CAMERA_APERTURE: f32 = 60.0;
const INITIAL_CAMERA_DISTANCE: f32 = MAX_Z * 2.0;

fn main_window(frame_receiver: FrameReceiver, commands: AtCmdSender, config: ConfigReceiver) {
    let mut frame_receiver = frame_receiver;

    let window = Window::new(WindowSettings {
        title: "Instanced Shapes!".to_string(),
        max_size: Some((2000, 2000)),
        ..Default::default()
    })
    .unwrap();
    let context = window.gl();

    let mut camera = Camera::new_perspective(
        window.viewport(),
        vec3(0.00, 0.0, INITIAL_CAMERA_DISTANCE), // camera position
        vec3(0.0, 0.0, 0.0),                      // camera target
        vec3(0.0, 1.0, 0.0),                      // camera up
        degrees(CAMERA_APERTURE),
        0.1,
        10000.0,
    );
    let mut control = OrbitControl::new(vec3(0.0, 0.0, 0.0), MAX_Z, MAX_Z * 4.0);

    let light0 = DirectionalLight::new(&context, 1.0, Srgba::WHITE, vec3(1.0, -1.0, 1.0));
    let light1 = DirectionalLight::new(&context, 1.0, Srgba::WHITE, vec3(-1.0, -1.0, 1.0));

    let mut frame: A010Frame = A010Frame::default();

    // Frame mesh object
    let positions = Vec::with_capacity(MAX_FRAME_SIZE);
    let indices = Vec::with_capacity(MAX_FRAME_SIZE * 3);
    let mut frame_mesh = CpuMesh {
        positions: Positions::F32(positions),
        indices: Indices::U16(indices),
        colors: Some(Vec::with_capacity(MAX_FRAME_SIZE)),
        ..Default::default()
    };

    let allowed_binn = BINN::allowed_values();
    let allowed_disp = DISP::allowed_values();
    let allowed_baud = BAUD::allowed_values();
    let allowed_unit = UNIT::allowed_values();
    let allowed_fps = FPS::allowed_values();

    let mut gui = three_d::GUI::new(&context);
    window.render_loop(move |mut frame_input| {
        let current_config = *config.borrow();
        let mut next_config = current_config;
        let mut panel_width = 0.0;

        gui.update(
            &mut frame_input.events,
            frame_input.accumulated_time,
            frame_input.viewport,
            frame_input.device_pixel_ratio,
            |gui_context| {
                use three_d::egui::*;
                SidePanel::left("side_panel").show(gui_context, |ui| {
                    use three_d::egui::*;

                    ui.heading("A010 configuration");

                    ComboBox::from_label("BINN")
                        .selected_text(current_config.binn.value_name())
                        .show_ui(ui, |ui| {
                            for v in allowed_binn.iter() {
                                ui.selectable_value(&mut next_config.binn, *v, v.value_name());
                            }
                        });

                    ComboBox::from_label("DISP")
                        .selected_text(current_config.disp.value_name())
                        .show_ui(ui, |ui| {
                            for v in allowed_disp.iter() {
                                ui.selectable_value(&mut next_config.disp, *v, v.value_name());
                            }
                        });

                    ComboBox::from_label("BAUD")
                        .selected_text(current_config.baud.value_name())
                        .show_ui(ui, |ui| {
                            for v in allowed_baud.iter() {
                                ui.selectable_value(&mut next_config.baud, *v, v.value_name());
                            }
                        });

                    ComboBox::from_label("UNIT")
                        .selected_text(current_config.unit.value_name())
                        .show_ui(ui, |ui| {
                            for v in allowed_unit.iter() {
                                ui.selectable_value(&mut next_config.unit, *v, v.value_name());
                            }
                        });

                    ComboBox::from_label("FPS")
                        .selected_text(current_config.fps.value_name())
                        .show_ui(ui, |ui| {
                            for v in allowed_fps.iter() {
                                ui.selectable_value(&mut next_config.fps, *v, v.value_name());
                            }
                        });
                });

                panel_width = gui_context.used_rect().width();
            },
        );
        let viewport = Viewport {
            x: (panel_width * frame_input.device_pixel_ratio) as i32,
            y: 0,
            width: frame_input.viewport.width
                - (panel_width * frame_input.device_pixel_ratio) as u32,
            height: frame_input.viewport.height,
        };
        camera.set_viewport(viewport);

        // Camera control must be after the gui update.
        control.handle_events(&mut camera, &mut frame_input.events);

        // Get frame data
        match frame_receiver.has_changed() {
            Ok(changed) => {
                if changed {
                    let new_frame = frame_receiver.borrow_and_update();
                    frame = *new_frame;
                }
            }
            Err(e) => {
                println!("frame receiver error: {}", e);
            }
        }

        // Handle config changes
        if next_config.binn != current_config.binn {
            println!("Setting BINN to {}", next_config.binn.value_name());
            commands
                .try_send(A010Cmd::Set(A010Arg::Binn(next_config.binn)))
                .unwrap();
        }
        if next_config.disp != current_config.disp {
            println!("Setting DISP to {}", next_config.disp.value_name());
            commands
                .try_send(A010Cmd::Set(A010Arg::Disp(next_config.disp)))
                .unwrap();
        }
        if next_config.baud != current_config.baud {
            println!("Setting BAUD to {}", next_config.baud.value_name());
            commands
                .try_send(A010Cmd::Set(A010Arg::Baud(next_config.baud)))
                .unwrap();
        }
        if next_config.unit != current_config.unit {
            println!("Setting UNIT to {}", next_config.unit.value_name());
            commands
                .try_send(A010Cmd::Set(A010Arg::Unit(next_config.unit)))
                .unwrap();
        }
        if next_config.fps != current_config.fps {
            println!("Setting FPS to {}", next_config.fps.value_name());
            commands
                .try_send(A010Cmd::Set(A010Arg::Fps(next_config.fps)))
                .unwrap();
        }

        // Build frame
        let render_frame = setup_frame(&frame, &mut frame_mesh);

        // Render everything
        let screen = frame_input.screen();
        screen.clear(ClearState::color_and_depth(0.8, 0.8, 0.8, 1.0, 1.0));
        if render_frame {
            let object = Gm::new(Mesh::new(&context, &frame_mesh), ColorMaterial::default());
            screen.render(&camera, &object, &[&light0, &light1]);
        }

        screen.write(|| gui.render()).unwrap();

        FrameOutput::default()
    });
}
