use bytes::Bytes;

enum DatabaseOperation {
    GET(Bytes),
    SET(Bytes, Bytes),
    DELETE(Bytes),
}
