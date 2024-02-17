from extract_yield import main
from load_s3 import main_s3
from prefect import flow

@flow
def pipe():
    main()
    main_s3()


if __name__ == '__main__':
    pipe()
