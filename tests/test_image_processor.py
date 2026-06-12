"""P4.01/P4.02 — EXIF orientation and resize behavior."""
import base64
import io

from PIL import Image

from src.image_processor import ImageProcessor


def make_processor(max_dimension=448):
    return ImageProcessor(max_dimension=max_dimension, patch_sizes=[14])


class TestCalculateDimensions:
    def test_small_image_not_upscaled(self):
        ip = make_processor(max_dimension=742)
        w, h = ip._calculate_dimensions(300, 200)
        # Nearest patch grid, NOT blown up to 742
        assert w <= 308 and h <= 210

    def test_large_image_downscaled_within_limit(self):
        ip = make_processor(max_dimension=448)
        w, h = ip._calculate_dimensions(4000, 3000)
        assert w <= 448 and h <= 448
        assert w % 14 == 0 and h % 14 == 0

    def test_never_exceeds_max_dimension(self):
        ip = make_processor(max_dimension=1280)
        for size in [(1281, 1281), (5000, 100), (1280, 1280), (13, 13)]:
            w, h = ip._calculate_dimensions(*size)
            assert w <= 1280 and h <= 1280, size
            assert w >= 14 and h >= 14, size


class TestExifOrientation:
    def test_sideways_jpeg_is_uprighted(self, tmp_path):
        # 600x200 landscape saved with Orientation=6 (rotate 90 CW to view):
        # a correct pipeline must emit a PORTRAIT (taller than wide) image.
        img = Image.new("RGB", (600, 200), "white")
        exif = img.getexif()
        exif[0x0112] = 6  # Orientation: Rotate 90 CW
        path = tmp_path / "rotated.jpg"
        img.save(path, exif=exif)

        ip = make_processor(max_dimension=448)
        encoded = ip.route_image(str(path))
        out = Image.open(io.BytesIO(base64.b64decode(encoded)))
        assert out.height > out.width, (
            f"EXIF orientation not applied: got {out.size}"
        )

    def test_normal_jpeg_unrotated(self, tmp_path):
        img = Image.new("RGB", (600, 200), "white")
        path = tmp_path / "plain.jpg"
        img.save(path)

        ip = make_processor(max_dimension=448)
        out = Image.open(io.BytesIO(base64.b64decode(ip.route_image(str(path)))))
        assert out.width > out.height
