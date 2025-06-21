import logging
import multiprocessing
import sys
import time


def main():
    """
    The main entry point for the application.
    Configures logging and starts the requested service.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    from producer.producer import run_producer
    from consumer.pipeline import run_consumer

    logging.info("Application starting...")

    producer_process = multiprocessing.Process(target=run_producer, name="Producer")

    consumer_args = (['--streaming'],)
    consumer_process = multiprocessing.Process(target=run_consumer, args=consumer_args, name="Consumer")

    producer_process.start()
    logging.info("Producer process started.")

    consumer_process.start()
    logging.info("Consumer process started.")

    try:
        while True:
            # Keep the main process alive to monitor child processes
            time.sleep(1)
            if not producer_process.is_alive():
                logging.warning("Producer process has terminated unexpectedly. Shutting down.")
                consumer_process.terminate()
                break
            if not consumer_process.is_alive():
                logging.warning("Consumer process has terminated unexpectedly. Shutting down.")
                producer_process.terminate()
                break
    except KeyboardInterrupt:
        logging.info("Ctrl+C received. Shutting down all processes...")
    finally:
        if producer_process.is_alive():
            producer_process.terminate()  # Send SIGTERM
            producer_process.join()  # Wait for it to close
            logging.info("Producer process terminated.")
        if consumer_process.is_alive():
            consumer_process.terminate()
            consumer_process.join()
            logging.info("Consumer process terminated.")
        logging.info("Application shut down complete.")


if __name__ == '__main__':
    main()
