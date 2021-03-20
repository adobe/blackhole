package archive

import (
	"io"
	"strings"

	"github.com/adobe/blackhole/lib/archive/az"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/adobe/blackhole/lib/archive/file"
	"github.com/adobe/blackhole/lib/archive/s3f"
	"github.com/pkg/errors"
)

type Archive interface {
	io.ReadWriteCloser
	Rotate() (err error)
	Flush() (err error)
	Name() string
}

func getProto(outDir string) string {

	if !strings.Contains(outDir, "://") || strings.HasPrefix(outDir, "file://") {
		return "file"
	} else {
		return strings.ToLower(outDir[:strings.Index(outDir, "://")])
	}
}

// NewArchive opens an archive file for write. Once rotated after calling Rotate(),
// new writes will go to a new file
// If outDir starts with "az://<container-name>/some/path/inside"
// or "s3://<bucket-name>/some/path/inside", the archive file would be uploaded to Azure Blobstore or S3
// respectively. Please note settings are not all similar.
// Azure side code expects to environment variables AZURE_STORAGE_ACCOUNT as well as
// AZURE_STORAGE_ACCESS_KEY . However S3 side expects a `aws configure` performed with default
// settings in ~/.aws/credentials. There is no support for "profiles" yet.
func NewArchive(outDir, prefix, extension string, options ...func(*common.BasicArchive) error) (rf Archive, err error) {

	switch getProto(outDir) {
	case "file":
		rf, err = file.NewArchive(outDir, prefix, extension, options...)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	case "az":
		rf, err = az.NewArchive(outDir, prefix, extension, options...)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
	case "s3":
		rf, err = s3f.NewArchive(outDir, prefix, extension, options...)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
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

	switch getProto(fileName) {
	case "file":
		rf, err = file.OpenArchive(fileName, bufferSize)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create local file")
		}
		return rf, nil
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
	return nil, errors.Errorf("Unsupported URL type")
}

func List(dir string) (files []string, err error) {

	switch getProto(dir) {
	case "file":
		files, err = file.List(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to list")
		}
		return files, nil
	case "az":
		files, err = az.List(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to list")
		}
		return files, nil
	case "s3":
		files, err = s3f.List(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to list")
		}
		return files, nil
	}
	return nil, errors.Errorf("Unsupported URL type")
}

func Delete(dir string, files []string) (err error) {

	switch getProto(dir) {
	case "file":
		err = file.Delete(dir, files)
		if err != nil {
			return errors.Wrapf(err, "Unable to list")
		}
		return nil
	case "az":
		err = az.Delete(dir, files)
		if err != nil {
			return errors.Wrapf(err, "Unable to list")
		}
		return nil
	case "s3":
		err = s3f.Delete(dir, files)
		if err != nil {
			return errors.Wrapf(err, "Unable to list")
		}
		return nil
	}
	return errors.Errorf("Unsupported URL type")
}
