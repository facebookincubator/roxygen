use std::fmt::Write;

use super::HpackHeader;

pub fn dump_refset(refset: &[u32]) -> String {
    let mut output = String::from("\n[");
    for value in refset {
        write!(output, "{value} ").expect("writing to a String cannot fail");
    }
    output.push_str("]\n");
    output
}

pub fn dump_header_vector(headers: &[HpackHeader]) -> String {
    let mut output = String::new();
    for header in headers {
        writeln!(output, "{header}").expect("writing to a String cannot fail");
    }
    output.push('\n');
    output
}

pub fn print_delta(first: &[HpackHeader], second: &[HpackHeader]) -> String {
    let mut output = String::from("\n");
    let mut first_index = 0;
    let mut second_index = 0;
    while first_index < first.len() && second_index < second.len() {
        if first[first_index] < second[second_index] {
            if first_index > 0 && first[first_index - 1] == first[first_index] {
                writeln!(output, " duplicate {}", first[first_index])
                    .expect("writing to a String cannot fail");
            } else {
                writeln!(output, " + {}", first[first_index])
                    .expect("writing to a String cannot fail");
            }
            first_index += 1;
        } else if first[first_index] > second[second_index] {
            writeln!(output, " - {}", second[second_index])
                .expect("writing to a String cannot fail");
            second_index += 1;
        } else {
            first_index += 1;
            second_index += 1;
        }
    }
    while first_index < first.len() {
        write!(output, " + {}", first[first_index]).expect("writing to a String cannot fail");
        if first_index > 0 && first[first_index - 1] == first[first_index] {
            output.push_str(" (duplicate)");
        }
        output.push('\n');
        first_index += 1;
    }
    while second_index < second.len() {
        writeln!(output, " - {}", second[second_index]).expect("writing to a String cannot fail");
        second_index += 1;
    }
    output
}

pub fn dump_binary(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        if byte.is_ascii_graphic() || *byte == b' ' {
            write!(output, "{byte:08b} {} ", char::from(*byte))
                .expect("writing to a String cannot fail");
        } else {
            write!(output, "{byte:08b}   ").expect("writing to a String cannot fail");
        }
    }
    output.push('\n');
    output
}
