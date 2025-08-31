fn main() {
    use std::env;

    let optimize_for_machine = match env::var("MACHINE") {
        Ok(val) if val == "1" => true,
        _ => false,
    };

    let mut io_build = cc::Build::new();
    io_build.include("native").file("native/io.c");
    if optimize_for_machine {
        io_build.flag("-march=native");
        io_build.flag("-O3");
    }
    io_build.compile("io");

    let mut hashmap_build = cc::Build::new();
    hashmap_build
        .include("native")
        .file("native/hashmap.h")
        .file("native/lib.c")
        .file("native/lib.h")
        .file("native/burning_map.h")
        .file("native/burning_map.c")
        .file("native/hashmap.c");
    if optimize_for_machine {
        hashmap_build.flag("-march=native");
        hashmap_build.flag("-O3");
    }
    hashmap_build.compile("hashmap");

    println!("cargo:rustc-link-lib=static=io");
    println!("cargo:rustc-link-lib=static=hashmap");
    println!("cargo:rustc-link-lib=static=uring");
}

