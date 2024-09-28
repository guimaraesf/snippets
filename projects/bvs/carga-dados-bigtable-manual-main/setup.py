import setuptools
import os

package_version = os.environ.get("APP_VERSION", "1.0.0")
app_artifact_id = os.environ.get("APP_ARTIFACT_ID", "carga-dados-manual")

setuptools.setup(
    name=app_artifact_id,
    version=package_version,
    packages=setuptools.find_packages(),
    python_requires=">=3.9.5",
)
