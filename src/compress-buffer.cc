#include <node.h>
#include <node_buffer.h>
#include <string.h>
#include <v8.h>
#include <math.h>
#include <nan.h>
#include <stdlib.h>
#include <assert.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#endif
#include <zlib.h>

// zlib magic something
#define WBITS 16+MAX_WBITS
#define WBITS_RAW -15

#define CHUNK 1024*100
#define HEADER_SIZE 10
#define FOOTER_SIZE 8
#define SPACER_SIZE 6

#define LOCAL_STR(name) Nan::New<v8::String>(name).ToLocalChecked()

namespace node_compress_buffer {

    static Nan::Persistent<v8::String> SYM_BODY(LOCAL_STR("body"));
    static Nan::Persistent<v8::String> SYM_BOUNDARY(LOCAL_STR("boundary"));
    static Nan::Persistent<v8::String> SYM_LEFT(LOCAL_STR("left"));
    static Nan::Persistent<v8::String> SYM_RIGHT(LOCAL_STR("right"));
    static Nan::Persistent<v8::String> SYM_LAST_BLOCK(LOCAL_STR("lastBlock"));
    static Nan::Persistent<v8::String> SYM_LAST_VALUE(LOCAL_STR("lastValue"));
    static Nan::Persistent<v8::String> SYM_TYPE(LOCAL_STR("type"));
    static Nan::Persistent<v8::String> SYM_OFFSETS(LOCAL_STR("offsets"));
    static Nan::Persistent<v8::String> SYM_LENGTH(LOCAL_STR("length"));
    static Nan::Persistent<v8::String> SYM_RAW_LENGTH(LOCAL_STR("rawLength"));
    static Nan::Persistent<v8::String> SYM_CRC(LOCAL_STR("crc"));
    static Nan::Persistent<v8::String> SYM_META(LOCAL_STR("meta"));
    static Nan::Persistent<v8::String> SYM_BUFFERS(LOCAL_STR("buffers"));
    static Nan::Persistent<v8::String> SYM_HEADER(LOCAL_STR("header"));
    static Nan::Persistent<v8::String> SYM_BUFFER(LOCAL_STR("Buffer"));

    unsigned char *tmpBody;

    static int meta_uncompress (char **dataIn, size_t bytesIn, char **dataBoundary, size_t *bytesBoundary, int *lastBlockPosition, int *lastBlockValue) {
        int last;
        int pos;

        z_stream strmUncompress;

        strmUncompress.zalloc = Z_NULL;
        strmUncompress.zfree = Z_NULL;
        strmUncompress.opaque = Z_NULL;
        strmUncompress.avail_in = 0;
        strmUncompress.next_in = Z_NULL;

        if (inflateInit2(&strmUncompress, WBITS_RAW) != Z_OK) {
            return -1;
        }

        //skipping header
        strmUncompress.next_in = ((unsigned char *) *dataIn) + HEADER_SIZE;
        strmUncompress.avail_in = bytesIn - HEADER_SIZE;

        //is there only one block in the stream?
        last = strmUncompress.next_in[0] & 1;
        if (last) {
            *lastBlockPosition = bytesIn - strmUncompress.avail_in;
            *lastBlockValue = 1;
        }

        for (;;) {
            strmUncompress.avail_out = CHUNK;
            strmUncompress.next_out = tmpBody;

            int ret = inflate(&strmUncompress, Z_BLOCK);

            assert(ret != Z_STREAM_ERROR);

            switch (ret) {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    inflateEnd(&strmUncompress);
                    return -2;
            }

            if (strmUncompress.data_type & 128) {
                if (last) {
                    break;
                }

                pos = strmUncompress.data_type & 7;

                if (pos != 0) {
                    pos = 0x100 >> pos;

                    last = strmUncompress.next_in[-1] & pos;
                    if (last) {
                        *lastBlockPosition = bytesIn - strmUncompress.avail_in - 1;
                        *lastBlockValue = pos;
                    }
                } else {
                    last = strmUncompress.next_in[0] & 1;
                    if (last) {
                        *lastBlockPosition = bytesIn - strmUncompress.avail_in;
                        *lastBlockValue = 1;
                    }
                }
            }
        }

        last = strmUncompress.next_in[-1];
        pos = strmUncompress.data_type & 7;

        if (pos == 0) {
            *(*dataBoundary + *bytesBoundary) = last;
            (*bytesBoundary)++;
        } else {
            last &= ((0x100 >> pos) - 1);

            if (pos & 1) {
                *(*dataBoundary + *bytesBoundary) = last;
                (*bytesBoundary)++;

                if (pos == 1) {
                    *(*dataBoundary + *bytesBoundary) = 0x00;
                    (*bytesBoundary)++;
                }

                memcpy((*dataBoundary + *bytesBoundary), "\0\0\xff\xff", 4);
                *bytesBoundary += 4;
            } else {
                switch (pos) {
                    case 6:
                        *(*dataBoundary + *bytesBoundary) = last | 0x08;
                        (*bytesBoundary)++;
                        last = 0;
                    case 4:
                        *(*dataBoundary + *bytesBoundary) = last | 0x20;
                        (*bytesBoundary)++;
                        last = 0;
                    case 2:
                        *(*dataBoundary + *bytesBoundary) = last | 0x80;
                        (*bytesBoundary)++;
                        *(*dataBoundary + *bytesBoundary) = 0x00;
                        (*bytesBoundary)++;
                }
            }
        }

        *lastBlockPosition = *lastBlockPosition - HEADER_SIZE;

        inflateEnd(&strmUncompress);

        return 0;
    }

    static int compress (char *dataIn, size_t bytesIn, int compressionLevel, char **dataOut, size_t *bytesOut) {
        size_t bytesDeflated = 0;

        if (compressionLevel < 0 || compressionLevel > 9) {
            compressionLevel = Z_DEFAULT_COMPRESSION;
        }

        z_stream strmCompress;
        strmCompress.zalloc = Z_NULL;
        strmCompress.zfree = Z_NULL;
        strmCompress.opaque = Z_NULL;

        if (deflateInit2(&strmCompress, compressionLevel, Z_DEFLATED, WBITS, 8L, Z_DEFAULT_STRATEGY) != Z_OK) {
            return -1;
        }

        bytesDeflated = deflateBound(&strmCompress, bytesIn);

        if (bytesDeflated < 1024) {
            bytesDeflated = 1024;
        }

        *dataOut = (char *) malloc(bytesDeflated);

        strmCompress.next_in = (Bytef *) dataIn;
        strmCompress.avail_in = bytesIn;
        strmCompress.next_out = (Bytef *) *dataOut;
        strmCompress.avail_out = bytesDeflated;

        if (deflate(&strmCompress, Z_NO_FLUSH) < Z_OK) {
            deflateEnd(&strmCompress);
            return -2;
        }

        deflate(&strmCompress, Z_FINISH);

        *bytesOut = strmCompress.total_out;

        deflateEnd(&strmCompress);

        return 0;
    }

    NAN_METHOD(onet_compress) {

        int compressionLevel = Z_DEFAULT_COMPRESSION;

        if (info.Length() < 1) {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        if (!node::Buffer::HasInstance(info[0])) {
            Nan::ThrowError("First argument must be a Buffer");
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        v8::Local<v8::Object> bufferIn = info[0]->ToObject();

        if (info.Length() > 1) {
            compressionLevel = info[1]->IntegerValue();
        }

        char *dataIn = node::Buffer::Data(bufferIn);
        size_t bytesIn = node::Buffer::Length(bufferIn);
        char *dataOut = 0;
        char *dataBoundary = (char *) malloc(SPACER_SIZE);
        size_t bytesOut = 0;
        size_t bytesBoundary = 0;
        int lastBlockPosition = 0;
        int lastBlockValue = 0;

        int status = compress(dataIn, bytesIn, compressionLevel, &dataOut, &bytesOut);

        if (status != 0) {
            if (dataOut) {
                free(dataOut);
            }

            char msg[30];
            sprintf(msg, "Unable to compress: %d", status);
            Nan::ThrowError(msg);
            return;
        }

        status = meta_uncompress(&dataOut, bytesOut, &dataBoundary, &bytesBoundary, &lastBlockPosition, &lastBlockValue);

        if (status != 0) {
            char msg[30];
            sprintf(msg, "Unable to uncompress: %d", status);
            Nan::ThrowError(msg);
            return;
        }

        unsigned int dataLength = bytesOut - HEADER_SIZE - FOOTER_SIZE - 1;

        v8::Local<v8::Object> result = Nan::New<v8::Object>();

        result->Set(Nan::New(SYM_BODY), Nan::CopyBuffer(dataOut, bytesOut).ToLocalChecked());

        result->Set(Nan::New(SYM_BOUNDARY), Nan::CopyBuffer(dataBoundary, bytesBoundary).ToLocalChecked());

        v8::Local<v8::Object> dataOffsets = Nan::New<v8::Object>();
        dataOffsets->Set(Nan::New(SYM_LEFT), Nan::New<v8::Integer>(HEADER_SIZE));
        dataOffsets->Set(Nan::New(SYM_RIGHT), Nan::New<v8::Integer>(HEADER_SIZE + dataLength));
        dataOffsets->Set(Nan::New(SYM_LAST_BLOCK), Nan::New<v8::Integer>(lastBlockPosition));

        v8::Local<v8::Object> meta = Nan::New<v8::Object>();
        meta->Set(Nan::New(SYM_OFFSETS), dataOffsets);
        meta->Set(Nan::New(SYM_LENGTH), Nan::New<v8::Integer>(static_cast<uint32_t>(bytesIn)));
        meta->Set(Nan::New(SYM_LAST_VALUE), Nan::New<v8::Integer>(lastBlockValue));
        meta->Set(Nan::New(SYM_RAW_LENGTH), Nan::New<v8::Integer>(static_cast<uint32_t>(bytesOut - HEADER_SIZE - FOOTER_SIZE)));
        meta->Set(Nan::New(SYM_CRC), Nan::CopyBuffer(dataOut + (bytesOut - FOOTER_SIZE), 4).ToLocalChecked());

        result->Set(Nan::New(SYM_META), meta);

        free(dataOut);
        free(dataBoundary);

        info.GetReturnValue().Set(result);
    }

    static NAN_METHOD(compress) {

        int compressionLevel = Z_DEFAULT_COMPRESSION;

        if (info.Length() < 1) {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        if (!node::Buffer::HasInstance(info[0])) {
            Nan::ThrowError("First argument must be a Buffer");
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        v8::Local<v8::Object> bufferIn = info[0]->ToObject();

        if (info.Length() > 1) {
            compressionLevel = info[1]->IntegerValue();
        }

        char *dataIn = node::Buffer::Data(bufferIn);
        size_t bytesIn = node::Buffer::Length(bufferIn);
        char *dataOut = 0;
        size_t bytesOut = 0;
        int status = compress(dataIn, bytesIn, compressionLevel, &dataOut, &bytesOut);

        if (status != 0) {
            if (dataOut) {
                free(dataOut);
            }
            Nan::ThrowError("Unable to compress");
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        v8::Handle<v8::Value> b = Nan::CopyBuffer(dataOut, bytesOut).ToLocalChecked();
        free(dataOut);
        info.GetReturnValue().Set(b);
    }

    static NAN_METHOD(estimate) {
        v8::Local<v8::Array> arr = v8::Local<v8::Array>::Cast(info[0]);
        int i = 0;
        int l = arr->Length();
        int sum = HEADER_SIZE + FOOTER_SIZE + ((l - 1) * SPACER_SIZE);

        for(; i < l; i++) {
            v8::Local<v8::Object> obj = arr->Get(i)->ToObject();

            if (!obj->Has(Nan::New(SYM_META))) {
                char msg[40];
                sprintf(msg, "ESTIMATE wrong object (no meta key) at: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            v8::Local<v8::Object> meta = obj->Get(Nan::New(SYM_META))->ToObject();

            if (!meta->Has(Nan::New(SYM_RAW_LENGTH))) {
                char msg[60];
                sprintf(msg, "ESTIMATE wrong object (no rawLength key) at: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            sum += meta->Get(Nan::New(SYM_RAW_LENGTH))->Uint32Value();
        }

        info.GetReturnValue().Set(Nan::New<v8::Integer>(sum));
    }

    static unsigned long reverseBytes (unsigned char *buf) {
        unsigned long v;

        v = *buf;
        v += (unsigned long) *(buf + 1) << 8;
        v += (unsigned long) *(buf + 2) << 16;
        v += (unsigned long) *(buf + 3) << 24;

        return v;
    }

    static NAN_METHOD(getCrc) {

        unsigned long crc = crc32(0L, Z_NULL, 0);
        unsigned long tot = 0;

        v8::Local<v8::Array> arr = v8::Local<v8::Array>::Cast(info[0]);

        int l = arr->Length();
        int i = 0;

        for (; i < l; i++) {
            v8::Local<v8::Object> obj = arr->Get(i)->ToObject();

            if (!obj->Has(Nan::New(SYM_META))) {
                char msg[40];
                sprintf(msg, "CRC32 wrong object (no meta key) at: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            v8::Local<v8::Object> meta = obj->Get(Nan::New(SYM_META))->ToObject();

            if (!meta->Has(Nan::New(SYM_CRC))) {
                char msg[40];
                sprintf(msg, "CRC32 wrong object (no crc key) at: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            v8::Local<v8::Value> objCrc = meta->Get(Nan::New(SYM_CRC));
            if (!node::Buffer::HasInstance(objCrc)) {
                char msg[40];
                sprintf(msg, "CRC32 is not a buffer at: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            v8::Local<v8::Object> bufCrc = objCrc->ToObject();

            if (node::Buffer::Length(bufCrc) != 4) {
                char msg[40];
                sprintf(msg, "CRC32 buffer has invalid length: %d", i);
                Nan::ThrowError(msg);
                info.GetReturnValue().Set(Nan::Undefined());
                return;
            }

            unsigned long tmpCrc = reverseBytes((unsigned char *) node::Buffer::Data(bufCrc));
            unsigned long tmpLen = meta->Get(Nan::New(SYM_LENGTH))->Uint32Value();

            crc = crc32_combine(crc, tmpCrc, tmpLen);
            tot += tmpLen;
        }

        v8::Local<v8::Object> data = Nan::New<v8::Object>();
        data->Set(Nan::New(SYM_CRC), Nan::CopyBuffer((char *) &crc, 4).ToLocalChecked());
        data->Set(Nan::New(SYM_LENGTH), Nan::CopyBuffer((char *) &tot, 4).ToLocalChecked());

        info.GetReturnValue().Set(data);
    }

    NAN_METHOD(uncompress) {

        if (info.Length() < 1) {
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        if (!node::Buffer::HasInstance(info[0])) {
            Nan::ThrowError("First argument must be a Buffer");
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        z_stream strmUncompress;

        strmUncompress.zalloc=Z_NULL;
        strmUncompress.zfree=Z_NULL;
        strmUncompress.opaque=Z_NULL;
        strmUncompress.avail_in = 0;
        strmUncompress.next_in = Z_NULL;

        int rci = inflateInit2(&strmUncompress, WBITS);

        if (rci != Z_OK) {
            Nan::ThrowError("zlib initialization error.");
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        v8::Local<v8::Object> bufferIn = info[0]->ToObject();

        strmUncompress.next_in = (Bytef*) node::Buffer::Data(bufferIn);
        strmUncompress.avail_in = node::Buffer::Length(bufferIn);

        Bytef  *bufferOut = NULL;
        uint32_t malloc_size=0;
        uint32_t currentPosition=0;

        int ret;

        do {
            Bytef *tmp = (Bytef*)malloc(CHUNK);
            strmUncompress.avail_out = CHUNK;
            strmUncompress.next_out = tmp;

            ret = inflate(&strmUncompress, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);
            switch (ret) {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    (void)inflateEnd(&strmUncompress);
                    if (bufferOut!=NULL) {
                        free(bufferOut);
                    }
                    if (tmp!=NULL) {
                        free(tmp);
                    }
                    info.GetReturnValue().Set(Nan::Undefined());
                    return;
            }

            uint32_t have = CHUNK - strmUncompress.avail_out;
            if (have>0) {
                bufferOut = (Bytef *) realloc(bufferOut, malloc_size+have);
                malloc_size=malloc_size+have;
            }

            memcpy(bufferOut+currentPosition, tmp, have);
            currentPosition+=have;
            free(tmp);
        } while (strmUncompress.avail_out == 0 && ret != Z_STREAM_END);

        inflateEnd(&strmUncompress);

        if (ret != Z_STREAM_END) {
            if (bufferOut!=NULL) {
                free(bufferOut);
            }
            info.GetReturnValue().Set(Nan::Undefined());
            return;
        }

        v8::Handle<v8::Value> b = Nan::CopyBuffer((char *)bufferOut, malloc_size).ToLocalChecked();
        free(bufferOut);
        info.GetReturnValue().Set(b);
    }

    void init(v8::Handle<v8::Object> target) {

        tmpBody = (unsigned char *) malloc(CHUNK);

        const unsigned char header[] = {0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff};

        v8::Handle<v8::Object> buffers = Nan::New<v8::Object>();
        buffers->Set(Nan::New(SYM_HEADER), Nan::CopyBuffer(reinterpret_cast<const char*>(header), 10).ToLocalChecked());

        target->Set(Nan::New(SYM_BUFFERS), buffers);

        Nan::SetMethod(target, "compress", compress);
        Nan::SetMethod(target, "uncompress", uncompress);
        Nan::SetMethod(target, "metaCompress", onet_compress);
        Nan::SetMethod(target, "getCrc", getCrc);
        Nan::SetMethod(target, "estimate", estimate);
    }

}

NODE_MODULE(compress_buffer_bindings, node_compress_buffer::init);
