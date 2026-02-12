fn main() {
    let proto_path = "../proto/bep/bep.proto";
    println!("cargo:rerun-if-changed={proto_path}");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("resolve protoc");
    std::env::set_var("PROTOC", protoc);

    prost_build::Config::new()
        .compile_protos(&[proto_path], &["../proto"])
        .expect("compile BEP protobuf definitions");
}
