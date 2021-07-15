fn main() {
    tonic_build::compile_protos("protocol/feeds.proto").unwrap();
}
