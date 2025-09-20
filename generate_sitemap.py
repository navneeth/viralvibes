#!/usr/bin/env python3
"""
Generate sitemap.xml for ViralVibes application.
This script creates a sitemap with all public routes and their last modified dates.
"""

import os
from datetime import datetime
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from xml.dom import minidom

# Base URL of your website
BASE_URL = "https://viralvibes.fyi"

# List of public routes to include in sitemap
ROUTES = [
    "/",  # Home page
    "/analyze",  # Analysis page
]


def prettify(elem):
    """Return a pretty-printed XML string for the Element."""
    rough_string = ET.tostring(elem, "utf-8")
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def generate_sitemap():
    """Generate sitemap.xml with all public routes."""
    # Create the root element
    urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    # Get the script's directory and resolve main.py path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    main_file = os.path.join(script_dir, "main.py")

    # Get the last modified time of the main application file, handle missing file gracefully
    try:
        last_modified = datetime.fromtimestamp(os.path.getmtime(main_file))
        last_modified_str = last_modified.strftime("%Y-%m-%d")
    except FileNotFoundError:
        print(f"Warning: {main_file} not found, using current date for lastmod")
        last_modified_str = datetime.now().strftime("%Y-%m-%d")

    # Add each route to the sitemap
    for route in ROUTES:
        url = ET.SubElement(urlset, "url")

        # Add the full URL
        loc = ET.SubElement(url, "loc")
        loc.text = urljoin(BASE_URL, route)

        # Add last modified date
        lastmod = ET.SubElement(url, "lastmod")
        lastmod.text = last_modified_str

        # Add change frequency
        changefreq = ET.SubElement(url, "changefreq")
        changefreq.text = "weekly"

        # Add priority
        priority = ET.SubElement(url, "priority")
        priority.text = "0.8" if route == "/" else "0.6"

    # Create the sitemap file in the public directory
    public_dir = os.path.join(script_dir, "public")
    sitemap_path = os.path.join(public_dir, "sitemap.xml")

    try:
        # Ensure public directory exists
        os.makedirs(public_dir, exist_ok=True)

        with open(sitemap_path, "w", encoding="utf-8") as f:
            f.write(prettify(urlset))
        print(f"Sitemap generated at {sitemap_path}")
    except IOError as e:
        print(f"Error writing sitemap: {e}")
        return False

    return True


if __name__ == "__main__":
    generate_sitemap()
