[33mcommit 34f4c2404cf8a4c6808808a00278269b55c7c71b[m[33m ([m[1;36mHEAD[m[33m -> [m[1;32mfeature-750-loading-bar-bulk-download[m[33m)[m
Author: Till Krebber <till.krebber@rli-institut.de>
Date:   Wed Oct 21 10:39:21 2026 +0200

    Update Math instead of Numpy

[1mdiff --git a/open_mastr/xml_download/utils_download_bulk.py b/open_mastr/xml_download/utils_download_bulk.py[m
[1mindex a24edd4..44e1367 100644[m
[1m--- a/open_mastr/xml_download/utils_download_bulk.py[m
[1m+++ b/open_mastr/xml_download/utils_download_bulk.py[m
[36m@@ -1,3 +1,4 @@[m
[32m+[m[32mimport math[m
 import os[m
 import re[m
 import shutil[m
[36m@@ -359,7 +360,7 @@[m [mdef full_download_without_unzip_http([m
     head = requests.head(r.url, headers={"User-Agent": USER_AGENT})[m
     content_length = head.headers.get("content-length")[m
     total_length = int([m
[31m-        np.ceil(int(content_length or 3100 * 1024 * 1024) / (1024 * 1024))[m
[32m+[m[32m        math.ceil(int(content_length or 3100 * 1024 * 1024) / (1024 * 1024))[m
     )[m
 [m
     with ([m
