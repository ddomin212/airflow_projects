from airflow.decorators import dag, task
import pendulum
from podcast_utils import get_podcast_episodes, get_episodes_info, get_episode_transcript, get_episode_summary, get_chatbot
from dotenv import load_dotenv
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "downloads"
load_dotenv()


@dag(dag_id="podcast_summary2", start_date=pendulum.datetime(2023, 8, 31), schedule_interval="@daily", catchup=False)
def podcast_summary2():

    create_database = BashOperator(
        task_id="create_database",
        bash_command="""
            rm -f episodes.db
            sqlite3 episodes.db "VACUUM;"
        """,
    )
    create_connection = BashOperator(
        task_id="create_connection",
        bash_command="""
            CONNECTION_EXISTS=$(airflow connections get 'podcasts')

            if [ -z "$CONNECTION_EXISTS" ]; then
                airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host 'episodes.db' 
            else
                echo "Connection already exists. Skipping..."
            fi
        """,
    )
    create_table = SqliteOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS episodes(
                link TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                published_date TEXT NOT NULL,
                description TEXT NOT NULL,
                download_url TEXT,
                transcript TEXT,
                ai_text TEXT
            );
        """,
        sqlite_conn_id="podcasts",
    )

    @task()
    def get_episodes():
        episodes = get_podcast_episodes(PODCAST_URL)
        print("Found {} episodes".format(len(episodes)))
        return episodes

    podcast_episodes = get_episodes()
    create_table.set_downstream(podcast_episodes)
    create_connection.set_downstream(create_table)
    create_database.set_downstream(create_connection)

    @task()
    def insert_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored = hook.get_pandas_df("SELECT * FROM episodes")
        new_episodes = get_episodes_info(episodes, stored)
        hook.insert_rows("episodes", new_episodes, target_fields=["link", "title", "filename", "published_date", "description", "download_url"])

    new_episodes = insert_episodes(podcast_episodes)

    @task()
    def speech_to_text(new_episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        untranscribed_episodes = hook.get_pandas_df("SELECT * from episodes")

        for _, row in untranscribed_episodes.iterrows():
            if (not row["transcript"]) or len(row["transcript"]) < 1000:
                # print(f"Transcribing {row['filename']}")
                # print("Current file path: ", row['download_url'])
                transcript = get_episode_transcript(row['download_url'])
                hook.run(f'UPDATE episodes SET transcript="{transcript}" WHERE filename="{row["filename"]}"')

    transcribed_episodes = speech_to_text(new_episodes)

    @task()
    def summarize_episodes(transcribed_episodes):
        chatbot = get_chatbot()
        hook = SqliteHook(sqlite_conn_id="podcasts")
        transcribed_episodes = hook.get_pandas_df("SELECT * from episodes")

        for _, row in transcribed_episodes.iterrows():
            if (not row["ai_text"]) or len(row["ai_text"]) < 100:
                print(f"Summarizing {row['filename']}")
                text = get_episode_summary(row["transcript"], chatbot)
                hook.run(f'UPDATE episodes SET ai_text="AI SUMMARY: {text}" WHERE filename="{row["filename"]}"')

    summarize_episodes(transcribed_episodes)

summary = podcast_summary2()