import xmltodict
import requests
import os
from gradio_client import Client

def get_podcast_episodes(PODCAST_URL):
    data = requests.get(PODCAST_URL)
    feed = xmltodict.parse(data.text)
    episodes = feed["rss"]["channel"]["item"][:5]
    return episodes


def get_episodes_info(episodes, stored):
    new_episodes = []
    for episode in episodes:
        if episode["link"] not in stored["link"].values:
            filename = episode["link"].split("/")[-1] + ".mp3"
            new_episodes.append(
                [episode["link"], episode["title"], filename, episode["pubDate"], episode["description"], episode["enclosure"]["@url"]]
            )
    return new_episodes

def get_episode_transcript(url):
    client = Client("https://sanchit-gandhi-whisper-jax.hf.space/")
    transcript = client.predict(
        url,	# str (filepath or URL to file) in 'Audio file' Audio component
        "transcribe",	# str in 'Task' Radio component
        False,	# bool in 'Return timestamps' Checkbox component
        api_name="/predict_1"
    )
    transcript = transcript[0].replace('"', "'")
    return transcript

def get_episode_summary(transcript, chatbot):
    prompt = f'summarize this text """{transcript}"""'
    response = ""
    for data in chatbot.ask(prompt):
        response = data["message"]
    text = response.replace('"', "'")
    return text

def get_chatbot():
    from revChatGPT.V1 import Chatbot
    chatbot = Chatbot(config={
        "access_token": os.getenv("OPEN_AI_TOKEN"),
    })
    return chatbot