package archive

import (
	"github.com/adobe/blackhole/lib/archive/az"
	"github.com/adobe/blackhole/lib/archive/file"
	"github.com/adobe/blackhole/lib/archive/s3f"
	"github.com/pkg/errors"
	"io"
	"strings"
)

type Archive interface {
	io.ReadWriteCloser
	Rotate() (err error)
	Flush() (err error)
	Name() string
}

// NewArchive opens an archive file for write. Once rotated after calling Rotate(),
// new writes will go to a new file
// If outDir starts with "az://<container-name>/some/path/inside"
// or "s3://<bucket-name>/some/path/inside", the archive file would be uploaded to Azure Blobstore or S3
// respectively. Please note settings are not all similar.
// Azure side code expects to environment variables AZURE_STORAGE_ACCOUNT as well as
// AZURE_STORAGE_ACCESS_KEY . However S3 side expects a `aws configure` performed with default
// settings in ~/.aws/credentials. There is no support for "profiles" yet.
func NewArchive(outDir, prefix, extension string, compress bool, bufferSize int) (rf Archive, err error) {

	if !strings.Contains(outDir, "://") || strings.HasPrefix(outDir, "file://") {
		rf, err = file.NewArchive(outDir, prefix, extension, compress, bufferSize)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	} else {
		proto := strings.ToLower(outDir[:strings.Index(outDir, "://")])
		switch proto {
		case "az":
			rf, err = az.NewArchive(outDir, prefix, extension, compress, bufferSize)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create local file")
			}
			return rf, nil
		case "s3":
			rf, err = s3f.NewArchive(outDir, prefix, extension, compress, bufferSize)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create local file")
			}
			return rf, nil
		}
	}
	return nil, errors.Errorf("Unsupported URL type")
}

// OpenArchive opens a single archive file for read. If outDir starts with "az://<container-name>/some/path/inside"
// or "s3://<bucket-name>/some/path/inside", the archive file would be uploaded to Azure Blobstore or S3
// respectively. Please note settings are not all similar.
// Azure side code expects to environment variables AZURE_STORAGE_ACCOUNT as well as
// AZURE_STORAGE_ACCESS_KEY . However S3 side expects a `aws configure` performed with default
// settings in ~/.aws/credentials. There is no support for "profiles" yet.

func OpenArchive(fileName string, bufferSize int) (rf Archive, err error) {
	if !strings.Contains(fileName, "://") || strings.HasPrefix(fileName, "file://") {
		rf, err = file.OpenArchive(fileName, bufferSize)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	} else {
		proto := strings.ToLower(fileName[:strings.Index(fileName, "://")])
		switch proto {
		case "az":
			rf, err = az.OpenArchive(fileName, bufferSize)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create local file")
			}
			return rf, nil
		case "s3":
			rf, err = s3f.OpenArchive(fileName, bufferSize)
			if err != nil {
				return nil, errors.Wrapf(err, "Unable to create local file")
			}
			return rf, nil
		}
	}
	return nil, errors.Errorf("Unsupported URL type")
}
