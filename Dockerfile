FROM clearlinux/tesseract-ocr:latest
COPY /home/user/tessdata/eng.traineddata /usr/share/tessdata

#docker build -t clearlinux/tesseract-ocr:latest .