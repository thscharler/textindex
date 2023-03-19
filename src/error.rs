use std::error::Error;
use std::fmt::{Debug, Formatter};

pub struct AppError {
    pub err: Box<dyn Error>,
}

impl Debug for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.err)
    }
}

impl<E: Error + 'static> From<E> for AppError {
    fn from(value: E) -> Self {
        AppError {
            err: Box::new(value),
        }
    }
}

impl AppError {
    pub fn into_err(self) -> Box<dyn Error> {
        self.err
    }
}
