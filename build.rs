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
    }
    io_build.flag("-O3");
    io_build.flag("-msse4.2"); // I do not know if it is usefull here. But since its already required by another dependency, adding the flag hopping the compiler will make miracles wont hurt.
    io_build.compile("io");
    let mut btree_build = cc::Build::new();
    btree_build
        .include("native")
        .file("native/vector.c")
        .file("native/hashset.c")
        .file("native/btree.c");
    if optimize_for_machine {
        btree_build.flag("-march=native");
    }
    btree_build.flag("-static");
    btree_build.flag("-msse4.2");
    btree_build.compile("btree");
    println!("cargo:rustc-link-lib=static=io");
    println!("cargo:rustc-link-lib=static=btree");
    println!("cargo:rustc-link-lib=static=uring");
}
