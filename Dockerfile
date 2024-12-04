FROM python:3.13-alpine

# Set working directory
WORKDIR /app

# Install Poetry globally
RUN pip install --no-cache-dir poetry

# Copy dependency files first (to leverage Docker cache)
COPY pyproject.toml poetry.lock* ./

# Configure Poetry and install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

# Copy the rest of the application
COPY src/ ./src/
COPY .cache ./

# Set default command
CMD ["poetry", "run", "python"]
