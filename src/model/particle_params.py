import yaml

def load_particle_params(path="config/particles_bottle.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
