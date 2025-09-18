# Extrator de Rodadas

## Overview
Extrator de Rodadas is a Python application designed to interact with a game URL, process game results, and send data to Supabase and AWS. It utilizes asynchronous programming with `asyncio` and `aiohttp` to efficiently handle network requests and data processing.

## Features
- Asynchronously collects game results from a specified URL.
- Sends processed data to Supabase for storage.
- Integrates with AWS for additional data handling.
- Implements strategies for decision-making based on game history.

## Installation

### Prerequisites
- Python 3.7 or higher
- Docker (optional, for containerized deployment)

### Setup
1. Clone the repository:
   ```
   git clone <repository-url>
   cd extrator-de-rodadas
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   - Copy `.env.example` to `.env` and fill in the required values.

## Usage
To run the application, execute the following command:
```
python -u src/extrator.py
```

## Running with Docker
To build and run the application in a Docker container, use the following commands:
```
docker build -t extrator-de-rodadas .
docker run --env-file .env extrator-de-rodadas
```

## Testing
To run the unit tests, use:
```
pytest tests/test_extrator.py
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.