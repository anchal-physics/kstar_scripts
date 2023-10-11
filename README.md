# kstar_scripts
KSTAR convinience scripts

## Installing conda environment

```
conda env create --file conda_env.yml
conda activate kstar
```

## Try running sample

```
cd sample
conda activate kstar
python ../get_mdsplus.channel.py -c remote_config.yml
```

This should create a test.h5 file locally that has the requested data.