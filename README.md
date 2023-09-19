# Data pipeline for summarizing Marketplace podcast episodes.
1. Install the astro CLI, as per here [here](https://docs.astronomer.io/astro/cli/install-cli?tab=linux#upgrade-the-cli).
2. Modify the .env file, paste the `accessToken` from [here](https://chat.openai.com/api/auth/session) as the value for `OPEN_AI_TOKEN`.
3. Open this folder in a terminal, and run `astro dev start`. Then open the airflow UI at `localhost:8080`.
4. Run any of the DAGs by clicking on it and then clicking the play button in the topright.