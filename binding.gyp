{
	"targets": [
		{
			"target_name": "compress_buffer_bindings",
			"sources": [ "src/compress-buffer.cc" ],
			'link_settings': {
				'libraries': [
				'-lz'
				]
			},
            "include_dirs": [
                "<!(node -e \"require('nan')\")"
            ],
            "cflags":  ["-D_FILE_OFFSET_BITS=64", "-D_LARGEFILE_SOURCE", "-Wall", "-O3"]

		}
	]
}
