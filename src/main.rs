use clap::Parser;
use futures::sink::SinkExt;
use tokio::{select, spawn};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut},
    codec::{Decoder, Encoder, Framed, FramedRead, LinesCodec},
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

impl Decoder for A010Codec {
    type Item = String;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        while src.remaining() > 0 {
            let byte = src.get_u8();
            if let Some(cmd) = self.handle_byte(byte) {
                return Ok(Some(cmd));
            }
        }
        Ok(None)
    }
}

impl Encoder<String> for A010Codec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: String,
        dst: &mut tokio_util::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        println!("CMD: {}", &item);

        dst.extend_from_slice(item.as_bytes());
        dst.put_u8('\r' as u8);
        Ok(())
    }
}

async fn handle_frames(frame_receiver: FrameReceiver) {
    let mut frame_receiver = frame_receiver;
    loop {
        match frame_receiver.changed().await {
            Ok(_) => {
                // Handle frame
                let frame = frame_receiver.borrow();
                println!("Received frame: size {}", frame.frame_size);
            }
            Err(e) => {
                eprintln!("frame receiver error: {}", e);
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
    let mut a010: A010CodecFramed = Framed::new(serial, codec);
    spawn(handle_frames(frame_receiver));

    let stdin = tokio::io::stdin();
    let mut reader = FramedRead::new(stdin, LinesCodec::new());

    loop {
        select! {
            command = reader.next() => {
                let command = command.transpose().unwrap();
                if let Some(cmd) = command {
                    if let Err(err) = a010.send(cmd).await {
                        eprintln!("Error sending command: {}", err);
                        return;
                    }
                } else {
                    println!("stdin stream terminated");
                    return;
                }
            }
            response = a010.next() => {
                let response = response.transpose().unwrap();
                if let Some(rsp) = response {
                    println!("RSP: {}", rsp);
                } else {
                    println!("response stream terminated");
                    return;
                }
            }
        };
    }
}
