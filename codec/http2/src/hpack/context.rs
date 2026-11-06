use roxygen_core::headers::HttpHeaderCode;

use super::HeaderName;
use super::HeaderTable;
use super::HpackHeader;
use super::StaticHeaderTable;

#[derive(Clone, Debug)]
pub struct HpackContext {
    table: HeaderTable,
    static_refs: u32,
    mutation_epoch: Option<u64>,
}

impl HpackContext {
    pub fn new(table_size: u32) -> Self {
        Self {
            table: HeaderTable::new(table_size),
            static_refs: 0,
            mutation_epoch: Some(0),
        }
    }

    pub fn get_index(&mut self, header: &HpackHeader) -> (u32, u32) {
        self.get_index_parts(&header.name, &header.value, true)
    }

    pub fn get_index_parts(
        &mut self,
        name: &HeaderName,
        value: &[u8],
        check_dynamic_table: bool,
    ) -> (u32, u32) {
        let check_static = if value.is_empty() {
            name.is_common_header()
        } else {
            StaticHeaderTable::is_header_code_in_table_with_non_empty_value(name.code())
        };
        let mut static_index = (0, 0);
        if check_static {
            static_index = StaticHeaderTable::get_index(name, value);
            if static_index.0 != 0 {
                self.static_refs = self.static_refs.saturating_add(1);
                return (static_index.0, 0);
            }
        }

        let dynamic_index = if check_dynamic_table && self.table.capacity() > 0 {
            self.table.get_index_parts(name, value)
        } else {
            (0, 0)
        };
        if dynamic_index.0 != 0 {
            return (self.dynamic_to_global(dynamic_index.0), 0);
        }
        if static_index.1 != 0 {
            self.static_refs = self.static_refs.saturating_add(1);
            return (0, static_index.1);
        }
        if name.code() == HttpHeaderCode::Date {
            self.static_refs = self.static_refs.saturating_add(1);
            return (0, StaticHeaderTable::name_index(name));
        }
        if dynamic_index.1 != 0 {
            return (0, self.dynamic_to_global(dynamic_index.1));
        }
        if !check_static && name.is_common_header() {
            let index = StaticHeaderTable::name_index(name);
            if index != 0 {
                self.static_refs = self.static_refs.saturating_add(1);
                return (0, index);
            }
        }
        (0, 0)
    }

    pub(super) fn get_unknown_index_parts(&self, name: &[u8], value: &[u8]) -> (u32, u32) {
        if self.table.capacity() == 0 {
            return (0, 0);
        }
        let (index, name_index) = self.table.get_unknown_index_parts(name, value);
        let to_global = |index| {
            if index == 0 {
                0
            } else {
                self.dynamic_to_global(index)
            }
        };
        (to_global(index), to_global(name_index))
    }

    pub fn name_index(&mut self, name: &HeaderName) -> u32 {
        let index = StaticHeaderTable::name_index(name);
        if index != 0 {
            self.static_refs = self.static_refs.saturating_add(1);
            return index;
        }
        let index = self.table.name_index(name);
        if index == 0 {
            0
        } else {
            self.dynamic_to_global(index)
        }
    }

    pub fn is_static(index: u32) -> bool {
        index <= StaticHeaderTable::get().len() as u32
    }

    pub fn get_header(&mut self, index: u32) -> Option<&HpackHeader> {
        if Self::is_static(index) {
            self.static_refs = self.static_refs.saturating_add(1);
            StaticHeaderTable::get_header(index)
        } else {
            let dynamic = self.global_to_dynamic(index);
            self.table.get_header(dynamic)
        }
    }

    pub fn table(&self) -> &HeaderTable {
        &self.table
    }

    pub fn table_mut(&mut self) -> &mut HeaderTable {
        self.mutation_epoch = self.mutation_epoch.and_then(|epoch| epoch.checked_add(1));
        &mut self.table
    }

    pub(super) const fn mutation_epoch(&self) -> Option<u64> {
        self.mutation_epoch
    }

    pub(super) fn replay_static_refs(&mut self, count: u32) {
        self.static_refs = self.static_refs.saturating_add(count);
    }

    pub const fn static_refs(&self) -> u32 {
        self.static_refs
    }

    pub fn seed_header_table(&mut self, headers: impl IntoIterator<Item = HpackHeader>) {
        for header in headers {
            assert!(
                self.table_mut().add(header),
                "seed header must fit in the table"
            );
        }
    }

    fn dynamic_to_global(&self, index: u32) -> u32 {
        index + StaticHeaderTable::get().len() as u32
    }

    fn global_to_dynamic(&self, index: u32) -> u32 {
        index - StaticHeaderTable::get().len() as u32
    }
}

#[cfg(test)]
mod memo_tests {
    use super::*;

    #[test]
    fn mutation_epoch_covers_seeding_and_capacity_and_never_wraps() {
        let mut context = HpackContext::new(4096);
        let empty = context.mutation_epoch();
        context.seed_header_table([HpackHeader::new("x-seeded", "value")]);
        assert_ne!(context.mutation_epoch(), empty);
        let seeded = context.mutation_epoch();
        context.table_mut().set_capacity(4096);
        assert_ne!(context.mutation_epoch(), seeded);
        context.mutation_epoch = Some(u64::MAX);
        context.table_mut().clear();
        assert_eq!(context.mutation_epoch(), None);
        context.seed_header_table([HpackHeader::new("x-new", "value")]);
        assert_eq!(context.mutation_epoch(), None);
    }
}
