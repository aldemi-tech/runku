//! Canonical values and ordered index keys shared by every Runku storage adapter.
//!
//! The crate owns persistent binary formats but has no dependency on a database, filesystem,
//! transport, JavaScript runtime, or `SaaS` service.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod index;
mod stored;
mod value;

pub use index::{IndexKey, IndexKeyError, IndexKeyPrefix, IndexValue};
pub use stored::{
    STORED_VALUE_FORMAT_VERSION, STORED_VALUE_MAX_BYTES, StoredValueError, decode_stored_value,
    encode_stored_value,
};
pub use value::{
    CanonicalValue, FiniteF64, NonFiniteFloatError, ParseTypedIdError, TimestampMicros, TypedId,
};
