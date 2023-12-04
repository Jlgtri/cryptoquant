# CryptoQuant (API disabled)

## Overview

CryptoQuant is a Python software designed to fetch cryptocurrency market data from the CryptoQuant website using the `aiohttp` library. The retrieved data is then stored in a PostgreSQL database using the `sqlalchemy` package. The software utilizes the `anyio` library for high-level asynchronous operations and `asyncclick` for seamless integration as a command-line interface (CLI). Additionally, the stored data is processed using neural networks with the help of the `pandas`, `scikit-learn`, and `matplotlib` libraries.

## Features

- **Data Fetching:** Retrieve real-time cryptocurrency market data from the CryptoQuant website.
- **Database Integration:** Save fetched data to a PostgreSQL database using a purely Python-written database schema.
- **Asynchronous Operations:** Leverage `anyio` for high-level asynchronous operations to optimize data fetching and processing.
- **CLI Integration:** Use `asyncclick` for a user-friendly command-line interface to interact with the software.
- **Data Processing:** Apply neural network algorithms for in-depth analysis of the stored cryptocurrency data using `pandas`, `scikit-learn`, and `matplotlib`.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Jlgtri/cryptoquant.git
   ```

2. Navigate to the project directory:

   ```bash
   cd cryptoquant
   ```

3. Create a virtual environment (optional but recommended):

   ```bash
   python -m venv venv
   ```

4. Activate the virtual environment:

   - On Windows:

   ```bash
   .\venv\Scripts\activate
   ```

   - On Unix or MacOS:

   ```bash
   source venv/bin/activate
   ```

5. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface (CLI)

```bash
python -m bin.main --help
```

## License

This software is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
