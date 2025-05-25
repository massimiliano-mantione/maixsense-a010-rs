use clap::Parser;
use futures::sink::SinkExt;
use three_d::*;
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

async fn handle_at_commands(a010: A010CodecFramed) {
    let mut a010 = a010;

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
    spawn(handle_at_commands(a010));

    main_window(frame_receiver);
}

const FRAME_WITH: f32 = 1000.0;
const FRAME_HEIGHT: f32 = 1000.0;
const FRAME_DISTANCE_UNIT: f32 = 10.0;

fn build_frame_transforms(frame: &A010Frame) -> Vec<Mat4> {
    let side = match frame.frame_size {
        10000 => 100,
        2500 => 50,
        625 => 25,
        _ => return Vec::new(),
    };

    let dw = FRAME_WITH / side as f32;
    let dh = FRAME_HEIGHT / side as f32;

    let x0 = -FRAME_WITH / 2.0;
    let y0 = -FRAME_HEIGHT / 2.0;

    let mut transformations = Vec::new();
    for ix in 0..side {
        for iy in 0..side {
            let x = x0 + (ix as f32 * dw);
            let y = y0 + (iy as f32 * dh);
            let i = ix + iy * side;
            let z = frame.data[i] as f32 * -FRAME_DISTANCE_UNIT;
            transformations.push(Mat4::from_translation(vec3(x, y, z)));
        }
    }
    transformations
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
            let z = frame.data[i] as f32 * -FRAME_DISTANCE_UNIT;
            let ax = ax0 + (ix as f32 * dw);
            let ay = ay0 + (iy as f32 * dh);
            let x = ax.to_radians().sin() * z * FRAME_DISTANCE_SCALE;
            let y = ay.to_radians().sin() * z * FRAME_DISTANCE_SCALE;
            positions.push(vec3(x, y, z));
        }
    }

    true
}

const CAMERA_APERTURE: f32 = 60.0;
const INITIAL_CAMERA_DISTANCE: f32 = 1000.0;

fn main_window(frame_receiver: FrameReceiver) {
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
    let mut control = OrbitControl::new(vec3(0.0, 0.0, 0.0), 1.0, 1000.0);

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

    // Initial properties of the example, 2 cubes per side and non instanced.
    let mut side_count = 2;

    let mut gui = three_d::GUI::new(&context);
    window.render_loop(move |mut frame_input| {
        // Gui panel to control the number of cubes and whether or not instancing is turned on.
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
                    ui.heading("Debug Panel");
                    ui.add(
                        Slider::new(&mut side_count, 1..=25).text("Number of cubes at each side."),
                    );
                    ui.add(Label::new(
                        "Increase the cube count until the cubes don't rotate \
                                       smoothly anymore, then toggle on instancing. The rotations \
                                       should become smooth again.",
                    ));
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
                eprintln!("frame receiver error: {}", e);
            }
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
