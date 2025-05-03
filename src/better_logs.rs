use chrono::Local;
use std::io::{self, Write};

pub fn logerr(args: std::fmt::Arguments) {
    let now = Local::now();
    let timestamp = now.format("%Y/%m/%d %H:%M:%S");
    let _ = writeln!(io::stderr(), "{} /// {}", timestamp, args);
}

#[macro_export]
macro_rules! logerr {
    ($($arg:tt)*) => {
        $crate::better_logs::logerr(::std::format_args!($($arg)*))
    };
}
