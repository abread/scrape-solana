use std::io::{self, Write};

pub trait IOTransformer {
    type Error: std::error::Error + Send + Sync + 'static;
    type Reader<R>: io::Read
    where
        R: io::BufRead;
    type Writer<W>: io::Write
    where
        W: io::Write;

    /// Read from a wrapped reader using the provided function.
    fn wrap_read<R: io::BufRead, T, E>(
        &self,
        orig_reader: R,
        read_fn: impl FnOnce(&mut Self::Reader<R>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>;

    /// Write to a wrapped writer using the provided function.
    /// The writer is flushed after the write_fn finishes successfully.
    fn wrap_write<W: io::Write, T, E>(
        &self,
        orig_writer: W,
        write_fn: impl FnOnce(&mut Self::Writer<W>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>;
}

impl IOTransformer for () {
    type Error = io::Error;
    type Reader<R> = R where R: io::BufRead;
    type Writer<W> = W where W: io::Write;

    fn wrap_read<R: io::BufRead, T, E>(
        &self,
        mut orig_reader: R,
        read_fn: impl FnOnce(&mut Self::Reader<R>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>,
    {
        read_fn(&mut orig_reader)
    }

    fn wrap_write<W: io::Write, T, E>(
        &self,
        mut orig_writer: W,
        write_fn: impl FnOnce(&mut Self::Writer<W>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>,
    {
        let res = write_fn(&mut orig_writer)?;
        orig_writer.flush()?;
        Ok(res)
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
    type Reader<R> = zstd::stream::zio::Reader<R, zstd::stream::raw::Decoder<'static>> where R: io::BufRead;
    type Writer<W> = zstd::stream::zio::Writer<W, zstd::stream::raw::Encoder<'static>> where W: io::Write;

    fn wrap_read<R: io::BufRead, T, E>(
        &self,
        orig_reader: R,
        read_fn: impl FnOnce(&mut Self::Reader<R>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>,
    {
        use zstd::stream::raw::Decoder;
        use zstd::stream::zio::Reader;

        let decoder = if let Some(dictionary) = self.dictionary.as_ref() {
            Decoder::with_dictionary(dictionary)
        } else {
            Decoder::new()
        }?;

        let mut reader = Reader::new(orig_reader, decoder);

        read_fn(&mut reader)
    }

    fn wrap_write<W: io::Write, T, E>(
        &self,
        orig_writer: W,
        write_fn: impl FnOnce(&mut Self::Writer<W>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Self::Error>,
    {
        use zstd::stream::raw::Encoder;
        use zstd::stream::zio::Writer;

        let encoder = if let Some(dictionary) = self.dictionary.as_ref() {
            Encoder::with_dictionary(self.level, dictionary)
        } else {
            Encoder::new(self.level)
        }?;

        let mut writer = Writer::new(orig_writer, encoder);

        let result = write_fn(&mut writer)?;
        writer.finish()?;
        writer.flush()?;

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::IOTransformer;
    use super::ZstdTransformer;
    use std::io::{self, Read, Write};

    #[test]
    fn encode_decode_equal_unit() {
        encode_decode_equal(())
    }

    #[test]
    fn encode_decode_equal_zstd_default() {
        encode_decode_equal(ZstdTransformer::default())
    }

    fn encode_decode_equal(io_transformer: impl IOTransformer<Error = io::Error>) {
        let data = vec![42u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42];
        let mut pipe = io::Cursor::new(Vec::<u8>::with_capacity(data.len()));

        {
            io_transformer
                .wrap_write(&mut pipe, |w| {
                    w.write_all(dbg!(&data))?;
                    w.flush()?;
                    Ok::<_, io::Error>(())
                })
                .unwrap();
        }

        pipe.set_position(0);

        let read_back = {
            io_transformer
                .wrap_read(&mut pipe, |r| {
                    let mut res = Vec::new();
                    r.read_to_end(&mut res)?;
                    Ok::<_, io::Error>(res)
                })
                .unwrap()
        };

        assert_eq!(data, read_back);
    }
}
