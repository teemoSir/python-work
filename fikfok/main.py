import cv2
import numpy as np
from PIL import Image
from PIL import ImageEnhance
from moviepy.editor import VideoFileClip, concatenate_videoclips,CompositeVideoClip
from moviepy.video.VideoClip import TextClip
import time
from PIL import ImageDraw, ImageFont


# 图像处理
def img_enhance(image, brightness=1, color=1,contrast=1,sharpness=1):
    # 亮度增强
    enh_bri = ImageEnhance.Brightness(image)
    if brightness:
        image = enh_bri.enhance(brightness)

    # 色度增强
    enh_col = ImageEnhance.Color(image)
    if color:
        image = enh_col.enhance(color)

    # 对比度增强
    enh_con = ImageEnhance.Contrast(image)
    if contrast:
        image = enh_con.enhance(contrast)

    # 锐度增强
    enh_sha = ImageEnhance.Sharpness(image)
    if sharpness:
        image = enh_sha.enhance(sharpness)


    # 垂直翻转
    #img_flip_tb = img.transpose(Image.FLIP_TOP_BOTTOM)
    # 水平翻转
    frame = image.transpose(Image.FLIP_LEFT_RIGHT)

    

    return frame

name = "2"
path = './data/'+name+'.mp4'
path2 = './outdata_cz/'+name+'_out.mp4'
path3='./outdata_cz/'+name+'_bz.mp4'
cap = cv2.VideoCapture(path)
success, _ = cap.read()
# 分辨率-宽度
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
# 分辨率-高度
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
# 总帧数
frame_counter = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
# FPS
fps = int(cap.get(cv2.CAP_PROP_FPS))
print(frame_counter)

video_writer = cv2.VideoWriter(path2, cv2.VideoWriter_fourcc('M', 'P', '4', 'V'), fps, (width, height), True)
index=1
while success:
    success, img1 = cap.read()

    try:
        index+=1
       
        if 0==0:

            # 图片旋转
            center = (img1.shape[1] // 2, img1.shape[0] // 2)
            M = cv2.getRotationMatrix2D(center, -3.0, 1.1)
            img1 = cv2.warpAffine(img1, M, (img1.shape[1], img1.shape[0]))

            

            # 添加水印
            watermark_text = '@卜甘旅行'[::-1]
            watermark_font = ImageFont.truetype('arial.ttf', 10)
            watermark_color = (230, 230, 230)  # 淡白色
            watermark_image = Image.fromarray(cv2.cvtColor(img1, cv2.COLOR_BGR2RGB))
            watermark_draw = ImageDraw.Draw(watermark_image)
            watermark_text_size = watermark_draw.textsize(watermark_text, font=watermark_font)
            watermark_position = (20, img1.shape[0] - 20 - watermark_text_size[1])
            watermark_draw.text(watermark_position, watermark_text, font=watermark_font, fill=watermark_color)
            img1 = cv2.cvtColor(np.array(watermark_image), cv2.COLOR_RGB2BGR)

            # 图像优化
            image = Image.fromarray(np.uint8(img1))  # 转换成PIL可以处理的格式
            img_enhanced = img_enhance(image, 1.1, 0.9, 1.2, 2.0)

            

            video_writer.write(np.asarray(img_enhanced))

            
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
    except:
        break


cap.release()
video_writer.release()


def video_duration_3(filename):
    start = time.time()
    cap = cv2.VideoCapture(filename)
    if cap.isOpened():
        rate = cap.get(5)
        frame_num = cap.get(7)
        duration = frame_num / rate
        end = time.time()
        spend = end - start
        print("获取视频时长方法3耗时：", spend)
        return duration
    return -1


lang = video_duration_3(path2)
# 剪辑50-60秒的音乐 00:00:50 - 00:00:60


video =CompositeVideoClip([VideoFileClip(path2).subclip(1.5,lang)])




videoclip = VideoFileClip(path)
audioclip = videoclip.audio
video = video.set_audio(audioclip)

# 写入剪辑完成的音乐
video.write_videofile(path3)

cv2.destroyAllWindows()