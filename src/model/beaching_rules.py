def should_beach(point, depth_m: float, dist_coast_m: float, cfg) -> bool:
    return (depth_m <= cfg["depth_thresh_m"]) or (dist_coast_m <= cfg["coastal_buffer_m"])
