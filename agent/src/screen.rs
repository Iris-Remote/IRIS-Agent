
use scrap::{Capturer, Display};
use image::{ImageBuffer, ImageEncoder, Rgba};
use std::{thread, time::Duration};
use std::io::{Cursor, ErrorKind::WouldBlock};
use base64;
use image::codecs::png::PngEncoder;
use image::ColorType;

pub(crate) fn takescreen() -> String {
    let display = Display::primary().expect("Couldn't find primary display.");
    let mut capturer = Capturer::new(display).expect("Couldn't begin capture.");
    let (w, h) = (capturer.width(), capturer.height());

    loop {
        match capturer.frame() {
            Ok(frame) => {
                let buffer: Vec<u8> = frame
                    .chunks(4)
                    .flat_map(|bgr| vec![bgr[2], bgr[1], bgr[0], 255u8])
                    .collect();
                let mut buf = Vec::new();
                let encoder = PngEncoder::new(&mut buf);
                encoder
                    .write_image(&buffer, w as u32, h as u32, ColorType::Rgba8.into())
                    .expect("Failed to encode PNG");
                return base64::encode(&buf);
            }
            Err(ref e) if e.kind() == WouldBlock => {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(e) => panic!("Error: {}", e),
        }
    }
}
