fn main() {
    cc::Build::new()
        .file("native/io.c")
        .include("native/liburing")
        .include("native")
        .compile("io");

    println!("cargo:rustc-link-lib=static=io");
}