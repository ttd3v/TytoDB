fn main() {
    cc::Build::new()
        .include("native")
        .file("native/io.c")
        .compile("io");
    cc::Build::new()
        .include("native")
        .file("native/hashmap.c")
        .compile("hashmap");

    println!("cargo:rustc-link-lib=static=io");
    println!("cargo:rustc-link-lib=static=hashmap");
    println!("cargo:rustc-link-lib=static=uring");

}

