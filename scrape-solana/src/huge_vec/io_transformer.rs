use std::{fmt::Debug, io};

pub trait IOTransformer {
    type Error: std::error::Error + Send + Sync + 'static;

    fn wrap_reader(&self, reader: impl io::BufRead) -> Result<impl io::Read, Self::Error>;
    fn wrap_writer(&self, writer: impl io::Write) -> Result<impl io::Write, Self::Error>;
}

impl IOTransformer for () {
    type Error = std::convert::Infallible;

    fn wrap_reader(&self, reader: impl io::BufRead) -> Result<impl io::Read, Self::Error> {
        Ok(reader)
    }

    fn wrap_writer(&self, writer: impl io::Write) -> Result<impl io::Write, Self::Error> {
        Ok(writer)
    }
}

pub struct ZstdTransformer {
    level: i32,
    dictionary: Option<Vec<u8>>,
}

impl ZstdTransformer {
    pub fn new(level: i32) -> Self {
        Self {
            level,
            dictionary: None,
        }
    }

    pub fn with_dictionary(level: i32, dictionary: Vec<u8>) -> Self {
        Self {
            level,
            dictionary: Some(dictionary),
        }
    }
}

impl Default for ZstdTransformer {
    fn default() -> Self {
        Self {
            level: 3,
            dictionary: None,
        }
    }
}

impl IOTransformer for ZstdTransformer {
    type Error = io::Error;

    fn wrap_reader(&self, reader: impl io::BufRead) -> Result<impl io::Read, Self::Error> {
        use zstd::stream::raw::Decoder;
        use zstd::stream::zio::Reader;

        let decoder = if let Some(dictionary) = self.dictionary.as_ref() {
            Decoder::with_dictionary(dictionary)
        } else {
            Decoder::new()
        }?;

        Ok(Reader::new(reader, decoder))
    }

    fn wrap_writer(&self, writer: impl io::Write) -> Result<impl io::Write, Self::Error> {
        use zstd::stream::raw::Encoder;
        use zstd::stream::zio::Writer;

        let encoder = if let Some(dictionary) = self.dictionary.as_ref() {
            Encoder::with_dictionary(self.level, dictionary)
        } else {
            Encoder::new(self.level)
        }?;

        Ok(Writer::new(writer, encoder))
    }
}

#[cfg(test)]
mod test {
    use super::IOTransformer;
    use super::ZstdTransformer;
    use std::io::Read;
    use std::io::{self, Write};

    #[test]
    fn encode_decode_equal_unit() {
        encode_decode_equal(())
    }

    #[test]
    fn encode_decode_equal_zstd_default() {
        encode_decode_equal(ZstdTransformer::default())
    }

    fn encode_decode_equal(io_transformer: impl IOTransformer) {
        let data = vec![42u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42];
        let mut pipe = io::Cursor::new(Vec::<u8>::with_capacity(data.len()));

        {
            let mut writer = io_transformer.wrap_writer(&mut pipe).unwrap();
            writer.write_all(&data).unwrap();
            writer.flush();
        }

        pipe.set_position(0);

        let read_back = {
            let mut reader = io_transformer.wrap_reader(&mut pipe).unwrap();
            let mut res = Vec::new();
            reader.read_to_end(&mut res);
            res
        };

        assert_eq!(data, read_back);
    }
}
