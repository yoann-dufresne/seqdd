from seqdd.utils.commands import curl_download
from tests import SeqddTest


class TestCurlDownload(SeqddTest):

    def test_includes_retry_output_and_url(self):
        cmd = curl_download('https://example.org/y', '/tmp/out', silent=True)
        self.assertIn('--retry', cmd)
        self.assertIn('-o /tmp/out', cmd)
        self.assertIn('"https://example.org/y"', cmd)
        self.assertIn('-s', cmd)

    def test_resume_flag_toggles(self):
        self.assertIn('-C -', curl_download('u', 'o', resume=True))
        self.assertNotIn('-C -', curl_download('u', 'o', resume=False))

    def test_silent_flag_toggles(self):
        self.assertIn(' -s', curl_download('u', 'o', silent=True))
        self.assertNotIn(' -s', curl_download('u', 'o', silent=False))
