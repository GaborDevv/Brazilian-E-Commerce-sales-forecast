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

def check_and_download_dataset(data_location, dataset):
    home_dir = get_home_directory()
    timestamp_dir = os.path.join(home_dir, '.kaggle-timestamps')
    os.makedirs(timestamp_dir, exist_ok=True)
    timestamp_file = os.path.join(timestamp_dir, f'{dataset.replace("/", "_")}.timestamp')

    current_timestamp = os.path.join(timestamp_dir, 'current.timestamp')

    # Get current dataset status
    with open(current_timestamp, 'w') as f:
        subprocess.run(['kaggle', 'datasets', 'metadata', dataset], stdout=f)

    # Check if dataset is updated
    if os.path.exists(timestamp_file):
        if filecmp.cmp(timestamp_file, current_timestamp, shallow=False):
            print("Dataset has not been updated.")
            os.remove(current_timestamp)
        else:
            print("Dataset has been updated. Downloading...")
            subprocess.run(['kaggle', 'datasets', 'download', dataset, '-p', data_location, '--unzip'])
            os.replace(current_timestamp, timestamp_file)
    else:
        print("No previous record found. Downloading dataset...")
        subprocess.run(['kaggle', 'datasets', 'download', dataset, '-p', data_location, '--unzip'])
        os.replace(current_timestamp, timestamp_file)

def main():
    parser = argparse.ArgumentParser(description='Check and download updated Kaggle dataset.')
    parser.add_argument('--data_location', type=str, required=False, default='raw_data', help='Directory where the dataset will be downloaded')
    args = parser.parse_args()

    # Kaggle dataset identifier
    dataset = "olistbr/brazilian-ecommerce"  # Change this to your dataset path on Kaggle

    install_requirements()
    check_and_download_dataset(args.data_location, dataset)

if __name__ == '__main__':
    main()