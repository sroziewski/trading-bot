import sys

from PyQt5.QtCore import Qt, QUrl, QTimer
from PyQt5.QtWebEngineWidgets import QWebEngineView, QWebEngineSettings
from PyQt5.QtWidgets import QApplication
from pytesseract import pytesseract

import cv2


class Screenshot(QWebEngineView):

    def capture(self, url, output_file):
        self.output_file = output_file
        self.load(QUrl(url))
        self.loadFinished.connect(self.on_loaded)
        # Create hidden view without scrollbars
        self.setAttribute(Qt.WA_DontShowOnScreen)
        self.page().settings().setAttribute(
            QWebEngineSettings.ShowScrollBars, False)
        self.show()

    def on_loaded(self):
        size = self.page().contentsSize().toSize()
        self.resize(size)
        # Wait for resize
        QTimer.singleShot(1000, self.take_screenshot)

    def take_screenshot(self):
        self.grab().save(self.output_file, b'PNG')
        self.app.quit()





app = QApplication(sys.argv)
s = Screenshot()
s.app = app
s.capture('https://www.tradingview.com/symbols/CRYPTOCAP-BTC.D/', 'webpage.png')
app.exec_()

image = cv2.imread('webpage.png')
y=338
x=19
h=32
w=97
crop = image[y:y+h, x:x+w]
# cv2.imshow('Image', crop)

# cv2.imwrite("cropped.png", crop)

pytesseract.tesseract_cmd = "C:\Program Files (x86)\Tesseract-OCR\\tesseract.exe"

print(pytesseract.image_to_string(crop))
# print(pytesseract.image_to_string(Image.open("cropped.png")))

# cv2.waitKey(0)

# sys.exit()