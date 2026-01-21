from pathlib import Path
import shutil
import kagglehub

DATASET = "gauthamp10/google-playstore-apps"

def main():
    dataset_path = Path(kagglehub.dataset_download(DATASET))
    print("Downloaded to kagglehub cache:", dataset_path)

    dest = Path("data/raw/playstore_kaggle")
    dest.mkdir(parents=True, exist_ok=True)

    copied = []
    for p in dataset_path.rglob("*"):
        if p.is_file():
            out = dest / p.name
            shutil.copy2(p, out)
            copied.append(out.name)

    print("Copied into:", dest.resolve())
    print("Files:")
    for name in sorted(set(copied)):
        print(" -", name)

if __name__ == "__main__":
    main()
