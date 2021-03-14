package archive

import (
	"github.com/adobe/blackhole/lib/archive/az"
	"github.com/adobe/blackhole/lib/archive/file"
	"github.com/pkg/errors"
	"io"
	"strings"
)

type Archive interface {
	io.ReadWriteCloser
	RotateArchiveFile() (err error)
	Flush() (err error)
	Name() string
}

func NewArchive(outDir, prefix, extension string, compress bool, bufferSize int) (rf Archive, err error) {

	extension = strings.TrimLeft(extension, ".") // allow extension to be specified as xyz or .xyz
	if !strings.Contains(outDir, "://") || strings.HasPrefix(outDir, "file://") {
		rf, err = file.NewArchiveFile(outDir, prefix, extension, compress, bufferSize)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	} else {
		proto := strings.ToLower(outDir[:strings.Index(outDir, "://")])
		switch proto {
		case "az":
			rf, err = az.NewArchiveFile(outDir, prefix, extension, compress, bufferSize)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create local file")
			}
			return rf, nil
		case "s3":
			panic("not implemented")
		}
	}
	return nil, errors.Errorf("Unsupported URL type")
}

func OpenArchive(fileName string, bufferSize int) (rf Archive, err error) {
	if !strings.Contains(fileName, "://") || strings.HasPrefix(fileName, "file://") {
		rf, err = file.OpenArchiveFile(fileName, bufferSize)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	}
	return nil, errors.Errorf("Unsupported URL type")
}
