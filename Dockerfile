# QueryQuest Chainlit app — Hugging Face Spaces (Docker SDK).
# Author: mohamedgamal04
FROM python:3.12-slim

# Hugging Face Spaces require running as a non-root user (uid 1000).
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR /home/user/app

# Install dependencies first for better layer caching.
COPY --chown=user pyproject.toml ./
COPY --chown=user src ./src
RUN pip install --user --no-cache-dir .

# App code.
COPY --chown=user chainlit_app.py ./

EXPOSE 7860
CMD ["chainlit", "run", "chainlit_app.py", "--host", "0.0.0.0", "--port", "7860"]
