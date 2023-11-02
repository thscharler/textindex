use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, AddAssign};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct BlkIdx(pub u32);

impl BlkIdx {
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for BlkIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Debug for BlkIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Add<u32> for BlkIdx {
    type Output = BlkIdx;

    fn add(self, rhs: u32) -> Self::Output {
        BlkIdx(self.0 + rhs)
    }
}

impl AddAssign<u32> for BlkIdx {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl PartialEq<u32> for BlkIdx {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for BlkIdx {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct FIdx(pub u32);

impl FIdx {
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for FIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Debug for FIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Add<u32> for FIdx {
    type Output = FIdx;

    fn add(self, rhs: u32) -> Self::Output {
        FIdx(self.0 + rhs)
    }
}

impl AddAssign<u32> for FIdx {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl PartialEq<u32> for FIdx {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for FIdx {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct FileId(pub u32);

impl FileId {
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({})", self.0)
    }
}

impl Debug for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({})", self.0)
    }
}

impl Add<u32> for FileId {
    type Output = FileId;

    fn add(self, rhs: u32) -> Self::Output {
        FileId(self.0 + rhs)
    }
}

impl AddAssign<u32> for FileId {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl PartialEq<u32> for FileId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for FileId {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct WordId(pub u32);

impl WordId {
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for WordId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({})", self.0)
    }
}

impl Debug for WordId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({})", self.0)
    }
}

impl Add<u32> for WordId {
    type Output = FileId;

    fn add(self, rhs: u32) -> Self::Output {
        FileId(self.0 + rhs)
    }
}

impl AddAssign<u32> for WordId {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl PartialEq<u32> for WordId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for WordId {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}
