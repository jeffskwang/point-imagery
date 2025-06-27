# point-imagery
Grab imagery around a point of interest

# directory
- `in/points_of_interest.csv`: This is a list of the name, latitude, longitude of each point.
- `src/get_imagery.py`: This is the main functions that will download the imagery around the points
- `out/`: directory with the imagery
- `tmp/`: directory with the rgb bands

# to run
1. Install conda environment: `conda env create -f environment.yaml`
2. Edit `in/points_of_interest.csv` if you want.
3. Change `radius` in `Snakefile` if you want.
4. Run the code with `snakemake -c1`
5. Check out the output in `out/`.