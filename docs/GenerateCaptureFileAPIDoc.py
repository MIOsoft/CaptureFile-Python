from lazydocs import generation
# seems to fail in python 3.8 but works in 3.10
generation.generate_docs(
    paths=["CaptureFile.CaptureFile"],
    ignored_modules=["ByteStream"],
    remove_package_prefix=True,
    watermark=False
)