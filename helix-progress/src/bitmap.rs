//! `RoaringBitmap` utilities for offset tracking.
//!
//! Provides extension traits for working with offsets relative to a base
//! watermark using `RoaringBitmap`.

#![allow(clippy::items_after_statements)]

use helix_core::Offset;
use roaring::RoaringBitmap;

/// Extension trait for `RoaringBitmap` offset operations.
///
/// Provides convenient methods for tracking committed offsets relative
/// to a base watermark.
pub trait OffsetBitmap {
    /// Sets the bit for the given offset relative to base.
    ///
    /// # Panics
    ///
    /// Panics if `offset < base`.
    fn set_offset(&mut self, base: Offset, offset: Offset);

    /// Checks if the bit is set for the given offset relative to base.
    ///
    /// Returns `false` if `offset < base`.
    fn has_offset(&self, base: Offset, offset: Offset) -> bool;

    /// Removes the bit for the given offset relative to base.
    ///
    /// No-op if `offset < base`.
    fn clear_offset(&mut self, base: Offset, offset: Offset);

    /// Returns the number of contiguous bits set starting from bit 0.
    ///
    /// This is useful for determining how far the watermark can advance.
    fn contiguous_count(&self) -> u64;

    /// Shifts all bits down by the given amount.
    ///
    /// Bits at positions `< shift_by` are discarded.
    /// Returns a new bitmap with shifted positions.
    fn shift_down(&self, shift_by: u64) -> RoaringBitmap;
}

impl OffsetBitmap for RoaringBitmap {
    fn set_offset(&mut self, base: Offset, offset: Offset) {
        assert!(
            offset.get() >= base.get(),
            "offset ({}) must be >= base ({})",
            offset.get(),
            base.get()
        );
        let index = offset.get() - base.get();
        #[allow(clippy::cast_possible_truncation)]
        self.insert(index as u32);
    }

    fn has_offset(&self, base: Offset, offset: Offset) -> bool {
        if offset.get() < base.get() {
            return false;
        }
        let index = offset.get() - base.get();
        #[allow(clippy::cast_possible_truncation)]
        self.contains(index as u32)
    }

    fn clear_offset(&mut self, base: Offset, offset: Offset) {
        if offset.get() >= base.get() {
            let index = offset.get() - base.get();
            #[allow(clippy::cast_possible_truncation)]
            self.remove(index as u32);
        }
    }

    fn contiguous_count(&self) -> u64 {
        let mut count = 0u64;
        // TigerStyle: Bound the loop.
        const MAX_CHECK: u64 = 10_000_000;
        while count < MAX_CHECK {
            #[allow(clippy::cast_possible_truncation)]
            if !self.contains(count as u32) {
                break;
            }
            count += 1;
        }
        count
    }

    fn shift_down(&self, shift_by: u64) -> Self {
        let mut new_bitmap = Self::new();
        for bit in self {
            let bit_u64 = u64::from(bit);
            if bit_u64 >= shift_by {
                let new_pos = bit_u64 - shift_by;
                #[allow(clippy::cast_possible_truncation)]
                new_bitmap.insert(new_pos as u32);
            }
        }
        new_bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_has_offset() {
        let mut bitmap = RoaringBitmap::new();
        let base = Offset::new(100);

        bitmap.set_offset(base, Offset::new(100));
        bitmap.set_offset(base, Offset::new(105));
        bitmap.set_offset(base, Offset::new(110));

        assert!(bitmap.has_offset(base, Offset::new(100)));
        assert!(!bitmap.has_offset(base, Offset::new(101)));
        assert!(bitmap.has_offset(base, Offset::new(105)));
        assert!(bitmap.has_offset(base, Offset::new(110)));

        // Below base returns false.
        assert!(!bitmap.has_offset(base, Offset::new(99)));
    }

    #[test]
    fn test_clear_offset() {
        let mut bitmap = RoaringBitmap::new();
        let base = Offset::new(0);

        bitmap.set_offset(base, Offset::new(5));
        assert!(bitmap.has_offset(base, Offset::new(5)));

        bitmap.clear_offset(base, Offset::new(5));
        assert!(!bitmap.has_offset(base, Offset::new(5)));

        // Clearing below base is a no-op.
        bitmap.set_offset(base, Offset::new(10));
        bitmap.clear_offset(Offset::new(20), Offset::new(10)); // 10 < 20.
        assert!(bitmap.has_offset(base, Offset::new(10)));
    }

    #[test]
    fn test_contiguous_count() {
        let mut bitmap = RoaringBitmap::new();

        // Empty bitmap.
        assert_eq!(bitmap.contiguous_count(), 0);

        // Bits 0, 1, 2.
        bitmap.insert(0);
        bitmap.insert(1);
        bitmap.insert(2);
        assert_eq!(bitmap.contiguous_count(), 3);

        // Gap at 3, then 4.
        bitmap.insert(4);
        assert_eq!(bitmap.contiguous_count(), 3); // Stops at gap.

        // Fill the gap.
        bitmap.insert(3);
        assert_eq!(bitmap.contiguous_count(), 5);
    }

    #[test]
    fn test_shift_down() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(5);
        bitmap.insert(10);
        bitmap.insert(15);

        let shifted = bitmap.shift_down(7);

        // 5 < 7, so discarded.
        assert!(!shifted.contains(0));
        // 10 - 7 = 3.
        assert!(shifted.contains(3));
        // 15 - 7 = 8.
        assert!(shifted.contains(8));

        assert_eq!(shifted.len(), 2);
    }

    #[test]
    #[should_panic(expected = "offset (99) must be >= base (100)")]
    fn test_set_offset_panics_below_base() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.set_offset(Offset::new(100), Offset::new(99));
    }
}
