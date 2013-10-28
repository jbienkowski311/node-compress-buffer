#include <node.h>
#include <node_buffer.h>
#include <string.h>
#include <v8.h>
#include <math.h>
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


using namespace v8;
using namespace node;

static Persistent<String> SYM_BODY;
static Persistent<String> SYM_LEFT;
static Persistent<String> SYM_RIGHT;
static Persistent<String> SYM_LAST;
static Persistent<String> SYM_TYPE;
static Persistent<String> SYM_OFFSETS;
static Persistent<String> SYM_LENGTH;
static Persistent<String> SYM_RAW_LENGTH;
static Persistent<String> SYM_CRC;
static Persistent<String> SYM_META;
static Persistent<String> SYM_BUFFERS;
static Persistent<String> SYM_HEADER;
static Persistent<String> SYM_ODD;

static Handle<Value> ThrowNodeError (const char* what = NULL) {
    return ThrowException(Exception::Error(String::New(what)));
}

static int meta_uncompress (char *dataIn, size_t bytesIn, int *dataType) {
    z_stream strmUncompress;

    strmUncompress.zalloc = Z_NULL;
    strmUncompress.zfree = Z_NULL;
    strmUncompress.opaque = Z_NULL;
    strmUncompress.avail_in = 0;
    strmUncompress.next_in = Z_NULL;

    //this can be further improved!
    //we should only do inflate if the the stream is compressed
    //and header skip should be done earlier
    if (inflateInit2(&strmUncompress, WBITS_RAW) != Z_OK) {
        return -1;
    }

    unsigned char *tmp = (unsigned char *) malloc(CHUNK);

    //skipping header
    strmUncompress.next_in = ((unsigned char *) dataIn) + HEADER_SIZE;
    strmUncompress.avail_in = bytesIn - HEADER_SIZE;

    //checking if stream is compressed, first byte - binary:
    //xxxxx001 - stream is NOT compressed
    //xxxxx011 - stream is compressed (using fixed huffman codes)
    //xxxxx111 - stream is compressed (using dynamic huffman codes)
    if ((*strmUncompress.next_in & 3) == 3 || (*strmUncompress.next_in & 5) == 5) {
        for (;;) {
            strmUncompress.avail_out = CHUNK;
            strmUncompress.next_out = tmp;

            int ret = inflate(&strmUncompress, Z_BLOCK);

            assert(ret != Z_STREAM_ERROR); 

            switch (ret) {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    inflateEnd(&strmUncompress);
                    if (tmp != NULL) {
                        free(tmp);
                    }

                    return -2;
            }

            if (strmUncompress.data_type & 128) {
                break;
            }
        }
        *dataType = strmUncompress.data_type;
    } else {
        *dataType = 128;
    }

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
        deflateReset(&strmCompress);
        return -2;
    }

    deflate(&strmCompress, Z_FINISH);

    *bytesOut = strmCompress.total_out;

    deflateReset(&strmCompress);

    return 0;
}

static Handle<Value> onet_compress (const Arguments &args) {
    HandleScope scope;

    int compressionLevel = Z_DEFAULT_COMPRESSION;

    if (args.Length() < 1) {
        return Undefined();
    }

    if (!Buffer::HasInstance(args[0])) {
        ThrowNodeError("First argument must be a Buffer");
        return Undefined();
    }

    Local<Object> bufferIn = args[0]->ToObject();

    if (args.Length() > 1) {
        compressionLevel = args[1]->IntegerValue();
    }

    char *dataIn = Buffer::Data(bufferIn);
    size_t bytesIn = Buffer::Length(bufferIn);
    char *dataOut = 0;
    size_t bytesOut = 0;
    int dataType = 0;
    int status = compress(dataIn, bytesIn, compressionLevel, &dataOut, &bytesOut);

    if (status != 0) {
        char msg[30];
        sprintf(msg, "Unable to compress: %d", status);
        ThrowNodeError(msg);
        return Undefined();
    }

    status = meta_uncompress(dataOut, bytesOut, &dataType);

    if (status != 0) {
        char msg[30];
        sprintf(msg, "Unable to uncompress: %d", status);
        ThrowNodeError(msg);
        return Undefined();
    }

    unsigned int dataLength = bytesOut - HEADER_SIZE - FOOTER_SIZE - 1;

    Local<Object> result = Object::New();

    Buffer *body = Buffer::New((char *) dataOut, bytesOut);
    result->Set(SYM_BODY, body->handle_);

    Local<Object> dataOffsets = Object::New();
    dataOffsets->Set(SYM_LEFT, Integer::New(HEADER_SIZE));
    dataOffsets->Set(SYM_RIGHT, Integer::New(HEADER_SIZE + dataLength));
    dataOffsets->Set(SYM_LAST, Integer::New(HEADER_SIZE + dataLength));

    Buffer *crc = Buffer::New((char *) dataOut + (bytesOut - FOOTER_SIZE), 4);

    Local<Object> meta = Object::New();
    meta->Set(SYM_TYPE, Integer::New(dataType));
    meta->Set(SYM_OFFSETS, dataOffsets);
    meta->Set(SYM_LENGTH, Integer::New(bytesIn));
    meta->Set(SYM_RAW_LENGTH, Integer::New(bytesOut - HEADER_SIZE - FOOTER_SIZE));
    meta->Set(SYM_CRC, crc->handle_);

    result->Set(SYM_META, meta);

    free(dataOut);

    return scope.Close(result);
}

static Handle<Value> compress (const Arguments& args) {
    HandleScope scope;

    int compressionLevel = Z_DEFAULT_COMPRESSION;

    if (args.Length() < 1) {
        return Undefined();
    }

    if (!Buffer::HasInstance(args[0])) {
        ThrowNodeError("First argument must be a Buffer");
        return Undefined();
    }

    Local<Object> bufferIn = args[0]->ToObject();

    if (args.Length() > 1) {
        compressionLevel = args[1]->IntegerValue();
    }

    char *dataIn = Buffer::Data(bufferIn);
    size_t bytesIn = Buffer::Length(bufferIn);
    char *dataOut = 0;
    size_t bytesOut = 0;
    int status = compress(dataIn, bytesIn, compressionLevel, &dataOut, &bytesOut);

    if (status != 0) {
        ThrowNodeError("Unable to compress");
        return Undefined();
    }

    Buffer *buff = Buffer::New(dataOut, bytesOut);

    return scope.Close(buff->handle_);
}

static Handle<Value> estimate (const Arguments &args) {
    HandleScope scope;

    Local<Array> arr = Local<Array>::Cast(args[0]);
    int i = 0;
    int l = arr->Length();
    int sum = HEADER_SIZE + FOOTER_SIZE + ((l - 1) * SPACER_SIZE);

    for(; i < l; i++) {
        Local<Object> obj = arr->Get(i)->ToObject();
        Local<Object> meta = obj->Get(SYM_META)->ToObject();

        sum += meta->Get(SYM_RAW_LENGTH)->Uint32Value();
    }

    return scope.Close(Integer::New(sum));
}

static unsigned long reverseBytes (unsigned char *buf) {
    unsigned long v;

    v = *buf;
    v += (unsigned long) *(buf + 1) << 8;
    v += (unsigned long) *(buf + 2) << 16;
    v += (unsigned long) *(buf + 3) << 24;

    return v;
}

static Handle<Value> getCrc (const Arguments &args) {
    HandleScope scope;

    unsigned long crc = crc32(0L, Z_NULL, 0);
    unsigned long tot = 0;

    Local<Array> arr = Local<Array>::Cast(args[0]);
    
    int l = arr->Length();
    int i = 0;

    for (; i < l; i++) {
        Local<Object> obj = arr->Get(i)->ToObject();
        Local<Object> meta = obj->Get(SYM_META)->ToObject();

        Local<Object> bufCrc = meta->Get(SYM_CRC)->ToObject();

        unsigned long tmpCrc = reverseBytes((unsigned char *) Buffer::Data(bufCrc));
        unsigned long tmpLen = meta->Get(SYM_LENGTH)->Uint32Value();

        crc = crc32_combine(crc, tmpCrc, tmpLen);
        tot += tmpLen;
    }

    Local<Object> data = Object::New();
    data->Set(SYM_CRC, Buffer::New((char *) &crc, 4)->handle_);
    data->Set(SYM_LENGTH, Buffer::New((char *) &tot, 4)->handle_);

    return scope.Close(data);
}

static Handle<Value> uncompress (const Arguments &args) {
    HandleScope scope;

    if (args.Length() < 1) {
        return Undefined();
    }

    if (!Buffer::HasInstance(args[0])) {
        ThrowNodeError("First argument must be a Buffer");
        return Undefined();
    }

    z_stream strmUncompress;
    
    strmUncompress.zalloc=Z_NULL;
    strmUncompress.zfree=Z_NULL;
    strmUncompress.opaque=Z_NULL;
    strmUncompress.avail_in = 0;
    strmUncompress.next_in = Z_NULL;

    int rci = inflateInit2(&strmUncompress, WBITS);

    if (rci != Z_OK) {
        ThrowNodeError("zlib initialization error.");
        return Undefined();
    }
    
    Local<Object> bufferIn=args[0]->ToObject();

    strmUncompress.next_in = (Bytef*) Buffer::Data(bufferIn);
    strmUncompress.avail_in = Buffer::Length(bufferIn);

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
                return Undefined();
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
        return Undefined();
    }

    Buffer *BufferOut=Buffer::New((char *)bufferOut, malloc_size);
    free(bufferOut);

    return scope.Close(BufferOut->handle_);
}

extern "C" void init (Handle<Object> target) {
    SYM_BODY = NODE_PSYMBOL("body");
    SYM_LEFT = NODE_PSYMBOL("left");
    SYM_RIGHT = NODE_PSYMBOL("right");
    SYM_LAST = NODE_PSYMBOL("last");
    SYM_TYPE = NODE_PSYMBOL("type");
    SYM_OFFSETS = NODE_PSYMBOL("offsets");
    SYM_LENGTH = NODE_PSYMBOL("length");
    SYM_RAW_LENGTH = NODE_PSYMBOL("rawLength");
    SYM_CRC = NODE_PSYMBOL("crc");
    SYM_META = NODE_PSYMBOL("meta");
    SYM_BUFFERS = NODE_PSYMBOL("buffers");
    SYM_HEADER = NODE_PSYMBOL("header");
    SYM_ODD = NODE_PSYMBOL("odd");

    char header[] = {0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff};
    Buffer *headerBuffer = Buffer::New(header, 10);

    char odd[] = {0x00, 0x00, 0xff, 0xff};
    Buffer *oddBuffer = Buffer::New(odd, 4);

    Handle<Object> buffers = Object::New();
    buffers->Set(SYM_HEADER, headerBuffer->handle_);
    buffers->Set(SYM_ODD, oddBuffer->handle_);

    target->Set(SYM_BUFFERS, buffers);

    NODE_SET_METHOD(target, "compress", compress);
    NODE_SET_METHOD(target, "uncompress", uncompress);
    NODE_SET_METHOD(target, "metaCompress", onet_compress);
    NODE_SET_METHOD(target, "getCrc", getCrc);
    NODE_SET_METHOD(target, "estimate", estimate);

}

NODE_MODULE(compress_buffer_bindings, init);

