import filecmp
import os
import sys
import argparse
import subprocess


def install_requirements():
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])


def get_home_directory():
    """Get the home directory using environment variables."""
    return os.environ.get('HOME') or os.environ.get('USERPROFILE')


def download_dataset(data_location, dataset):
    subprocess.run(['kaggle', 'datasets', 'download', dataset, '-p', data_location, '--unzip'])


def yes_or_no(value):
    if value.lower() not in ('yes', 'no'):
        raise argparse.ArgumentTypeError('Invalid answer, please enter "yes" or "no".')
    return value


def main():
    parser = argparse.ArgumentParser(description='Check and download updated Kaggle dataset.')
    parser.add_argument('--dataset_download', type=yes_or_no, required=True, help='Do you have an API key set up?')
    parser.add_argument('--data_location', type=str, required=False, default='raw_data',
                        help='Directory where the dataset will be downloaded')
    args = parser.parse_args()

    # Kaggle dataset identifier
    dataset = "olistbr/brazilian-ecommerce"  # Change this to your dataset path on Kaggle

    install_requirements()

    if args.dataset_download.lower() == 'yes':
        download_dataset(args.data_location, dataset)


if __name__ == '__main__':
    main()
