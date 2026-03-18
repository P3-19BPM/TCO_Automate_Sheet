from yt_dlp import YoutubeDL


def baixar_instagram(url: str, pasta_saida: str = "."):
    opcoes = {
        # nome do arquivo = título do vídeo
        "outtmpl": f"{pasta_saida}/%(title)s.%(ext)s",
        # tenta mp4, senão o melhor formato
        "format": "mp4/best",
    }

    with YoutubeDL(opcoes) as ydl:
        ydl.download([url])


if __name__ == "__main__":
    # Cole aqui o link do vídeo
    url = "https://www.instagram.com/reel/DQ7VIFxEQw4/?igsh=OGRjN3ZwdDQwN3l2"

    # Opcional: mude para a pasta onde deseja salvar
    pasta_saida = "downloads"

    baixar_instagram(url, pasta_saida)
    print("Download concluído!")
