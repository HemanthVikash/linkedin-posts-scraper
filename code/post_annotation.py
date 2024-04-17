from processor import PostsProcessor

def main():
    pp = PostsProcessor()
    pp.save(annotated=True)

if __name__ == "__main__":
    main()