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
//#define WBITS -15

#define CHUNK 1024*100


using namespace v8;
using namespace node;

Handle<Value> ThrowNodeError(const char* what = NULL) {
	return ThrowException(Exception::Error(String::New(what)));
}

    z_stream strmCompress;
Handle<Value> compress(const Arguments& args) {
	HandleScope scope;
	size_t bytesIn=0;
	size_t bytesCompressed=0;
	char *dataPointer=NULL;
	bool shouldFreeDataPointer=false;
	
	if (args.Length() < 1) { 
		return Undefined();
	}
	
	if (!Buffer::HasInstance(args[0])) {
		ThrowNodeError("First argument must be a Buffer");
		return Undefined();
	} 
	
	Local<Object> bufferIn=args[0]->ToObject();
	bytesIn=Buffer::Length(bufferIn);

	dataPointer=Buffer::Data(bufferIn);

	int compressionLevel = Z_DEFAULT_COMPRESSION;
	if (args.Length() > 1) { 
		compressionLevel = args[1]->IntegerValue();
        fprintf(stderr, "KOMPRESJA TO: %d\n", compressionLevel);
		if (compressionLevel < 0 || compressionLevel > 9) {
			compressionLevel = Z_DEFAULT_COMPRESSION;
		}
	}
	
	deflateParams(&strmCompress, compressionLevel, Z_DEFAULT_STRATEGY);
	
	bytesCompressed=compressBound(bytesIn);

	// compressBound mistakes when estimating extremely small data blocks (like few bytes), so 
	// here is the stub. Otherwise smaller buffers (like 10 bytes) would not compress.
	if (bytesCompressed<1024) {
		bytesCompressed=1024;
	}

	char *bufferOut=(char*) malloc(bytesCompressed);

	strmCompress.next_in=(Bytef*) dataPointer;
	strmCompress.avail_in=bytesIn;
	strmCompress.next_out=(Bytef*) bufferOut;
	strmCompress.avail_out=bytesCompressed;
	
	if (deflate(&strmCompress, Z_NO_FLUSH) < Z_OK) {
		deflateReset(&strmCompress);
		if (shouldFreeDataPointer) {
			free(dataPointer); 
			dataPointer = NULL;
		}
		return Undefined();
	}
	deflate(&strmCompress, Z_FINISH);
	bytesCompressed=strmCompress.total_out;
	deflateReset(&strmCompress);
	
	Buffer *BufferOut=Buffer::New(bufferOut, bytesCompressed);
	free(bufferOut);
	
	if (shouldFreeDataPointer) {
		free(dataPointer); 
		dataPointer = NULL;
	}

	return scope.Close(BufferOut->handle_);
}
	
Handle<Value> uncompress(const Arguments &args) {
	HandleScope scope;

	if (args.Length() < 1) {
		return Undefined();
	}
	
	if (!Buffer::HasInstance(args[0])) {
		ThrowNodeError("First argument must be a Buffer");
		return Undefined();
	}
	
	z_stream strmUncompress;
	
	strmUncompress.zalloc = Z_NULL;
	strmUncompress.zfree = Z_NULL;
	strmUncompress.opaque = Z_NULL;
	strmUncompress.avail_in = 0;
	strmUncompress.next_in = Z_NULL;

	int rci = inflateInit2(&strmUncompress, -15);

	if (rci != Z_OK) {
		ThrowNodeError("zlib initialization error.");
		return Undefined();
	}
	
	Local<Object> bufferIn = args[0]->ToObject();

    unsigned char *bufferData = (unsigned char *)Buffer::Data(bufferIn);
    unsigned int bufferLength = Buffer::Length(bufferIn);
    unsigned int length = 0;

    unsigned char *tmp = (unsigned char *) malloc(CHUNK);
    unsigned int headerLength = 10;
    unsigned int footerLength = 8;

    //pomijam naglowek
	strmUncompress.next_in = bufferData + headerLength;
	strmUncompress.avail_in = bufferLength - headerLength;

    fprintf(stderr, "SPAKOWANY? %x\n", *strmUncompress.next_in & 3);

    //sprawdzam czy blok jest skompresowany
    if ((*strmUncompress.next_in & 3) == 1) {

    } else {
        for (;;) {
            strmUncompress.avail_out = CHUNK;
            strmUncompress.next_out = tmp;

            int ret = inflate(&strmUncompress, Z_BLOCK);

            assert(ret != Z_STREAM_ERROR); 

    //        fprintf(stderr, "RET: %d\n", ret);
            switch (ret) {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    inflateEnd(&strmUncompress);
                    if (tmp != NULL) {
                        free(tmp);
                    }
                    return Undefined();
            }

            length += CHUNK - strmUncompress.avail_out;

    //        fprintf(stderr, "DATA_TYPE: %d\n", strmUncompress.data_type);

            if (strmUncompress.data_type & 128) {
                break;
            }
        }
    }

    //tutaj mozna po prostu odjac naglowek+stopke od streamu i bedziemy miec poprawna dlugosc
//    unsigned char *buf = bufferData  + (strmUncompress.next_in - bufferData);
//    unsigned int dataLength = buf - (bufferData + headerLength) - 1;
    unsigned int dataLength = bufferLength - headerLength - footerLength - 1;
//    fprintf(stderr, "DLUGOSC TO: %d\n", dataLength);

    free(tmp);

	inflateEnd(&strmUncompress);

    Local<Object> dataOffsets = Object::New();
    dataOffsets->Set(String::New("left"), Integer::New(headerLength));
    dataOffsets->Set(String::New("right"), Integer::New(headerLength + dataLength));
    dataOffsets->Set(String::New("last"), Integer::New(headerLength + dataLength));

    //bufferData + (bufferLength - 8), dlugosc = 4
    Buffer *crc = Buffer::New((char *) strmUncompress.next_in, 4);

    Local<Object> data = Object::New();
    data->Set(String::New("type"), Integer::New(strmUncompress.data_type));
    data->Set(String::New("offsets"), dataOffsets);
    data->Set(String::New("length"), Integer::New(length));
    data->Set(String::New("rawLength"), Integer::New(bufferLength - headerLength - footerLength));
    data->Set(String::New("crc"), crc->handle_);


	return scope.Close(data);
}

unsigned long reverseBytes (unsigned char *buf) {
    unsigned long v;

    v = *buf;
    v += (unsigned long) *(buf + 1) << 8;
    v += (unsigned long) *(buf + 2) << 16;
    v += (unsigned long) *(buf + 3) << 24;

    return v;
}

/*unsigned char *reverseBytes (unsigned long v) {
    unsigned char *buf[4] = {0x00, 0x00, 0x00, 0x00};

    buf[0] = (unsigned char *) (v & 0xff);
    buf[1] = (unsigned char *) ((v >> 8) & 0xff);
    buf[2] = (unsigned char *) ((v >> 16) & 0xff);
    buf[3] = (unsigned char *) ((v >> 24) & 0xff);

    return *buf;
}*/

Handle<Value> getCrc (const Arguments &args) {
    HandleScope scope;

    unsigned long crc = crc32(0L, Z_NULL, 0);
    unsigned long tot = 0;

    Local<Array> arr = Local<Array>::Cast(args[0]);
    
    int l = arr->Length();
    int i = 0;

    for (; i < l; i++) {
        Local<Object> obj = arr->Get(i)->ToObject();
        Local<Object> meta = obj->Get(String::New("meta"))->ToObject();

        Local<Object> bufCrc = meta->Get(String::New("crc"))->ToObject();

        unsigned long tmpCrc = reverseBytes((unsigned char *) Buffer::Data(bufCrc));
        unsigned long tmpLen = meta->Get(String::New("length"))->Uint32Value();

        crc = crc32_combine(crc, tmpCrc, tmpLen);
        tot += tmpLen;
    }

    Local<Object> data = Object::New();
    data->Set(String::New("crc"), Buffer::New((char *) &crc, 4)->handle_);
    data->Set(String::New("tot"), Buffer::New((char *) &tot, 4)->handle_);

    return scope.Close(data);
}

Handle<Value> uncompress2 (const Arguments &args) {
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

		ret = inflate(&strmUncompress, Z_BLOCK);
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


		if (strmUncompress.data_type & 64) {
			break;
		}

		uint32_t have = CHUNK - strmUncompress.avail_out;
		if (have>0) {
			bufferOut = (Bytef *) realloc(bufferOut, malloc_size+have);
			malloc_size=malloc_size+have;

			memcpy(bufferOut+currentPosition, tmp, have);
			currentPosition+=have;
		}

		free(tmp);
	} while (ret == Z_OK);

	inflateEnd(&strmUncompress);

	Buffer *BufferOut=Buffer::New((char *)bufferOut, malloc_size);
	free(bufferOut);

	return scope.Close(BufferOut->handle_);
}

Handle<Value> uncompress3(const Arguments &args) {
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

    HandleScope scope;
    return scope.Close(BufferOut->handle_);
}

extern "C" void
init (Handle<Object> target) {
	strmCompress.zalloc=Z_NULL;
	strmCompress.zfree=Z_NULL;
	strmCompress.opaque=Z_NULL;

	int rcd = deflateInit2(&strmCompress, Z_DEFAULT_COMPRESSION, Z_DEFLATED,  
		WBITS, 8L, Z_DEFAULT_STRATEGY);

	if (rcd != Z_OK) {
		ThrowNodeError("zlib initialization error.");
		return;
	}

	NODE_SET_METHOD(target, "compress", compress);
	NODE_SET_METHOD(target, "uncompress", uncompress);
	NODE_SET_METHOD(target, "uncompress2", uncompress2);
	NODE_SET_METHOD(target, "uncompress3", uncompress3);
	NODE_SET_METHOD(target, "getCrc", getCrc);
}

NODE_MODULE(compress_buffer_bindings, init);

