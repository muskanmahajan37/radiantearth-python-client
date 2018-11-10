import json
import os
import uuid

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from bravado.swagger_model import load_file, load_url
from simplejson import JSONDecodeError


from .aws.s3 import str_to_file
from .exceptions import RefreshTokenException
from .models import Analysis, MapToken, Project, Export, Datasource
from .settings import RV_TEMP_URI

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


base_git = 'https://raw.githubusercontent.com/'

SPEC_PATH = os.getenv(
    'RE_API_SPEC_PATH',
    base_git + 'radiantearth/radiantearth-python-client/dev/radiantearth/spec.yml',
)


class API(object):
    """Class to interact with Radiant Earth API"""

    def __init__(self, refresh_token=None, api_token=None,
                 host='api.radiant.earth', scheme='https'):
        """Instantiate an API object to make requests to Radiant Earth's REST API

        Args:
            refresh_token (str): optional token used to obtain an API token to
                                 make API requests
            api_token (str): optional token used to authenticate API requests
            host (str): optional host to use to make API requests against
            scheme (str): optional scheme to override making requests with
        """

        self.http = RequestsClient()
        self.scheme = scheme

        if urlparse(SPEC_PATH).netloc:
            spec = load_url(SPEC_PATH)
        else:
            spec = load_file(SPEC_PATH)

        self.app_host = host
        spec['host'] = host
        spec['schemes'] = [scheme]

        split_host = host.split('.')
        split_host[0] = 'tiles'
        self.tile_host = '.'.join(split_host)

        config = {'validate_responses': False}
        self.client = SwaggerClient.from_spec(spec, http_client=self.http,
                                              config=config)

        if refresh_token and not api_token:
            api_token = self.get_api_token(refresh_token)
        elif not api_token:
            raise Exception('Must provide either a refresh token or API token')

        self.api_token = api_token
        self.http.session.headers['Authorization'] = 'Bearer {}'.format(
            api_token)

    def get_api_token(self, refresh_token):
        """Retrieve API token given a refresh token

        Args:
            refresh_token (str): refresh token used to make a request for a new
                                 API token

        Returns:
            str
        """
        post_body = {'refresh_token': refresh_token}

        try:
            response = self.client.Authentication.post_tokens(
                refreshToken=post_body).future.result()
            return response.json()['id_token']
        except JSONDecodeError:
            raise RefreshTokenException('Error using refresh token, please '
                                        'verify it is valid')

    @property
    def map_tokens(self):
        """List map tokens a user has access to

        Returns:
            List[MapToken]
        """

        has_next = True
        page = 0
        map_tokens = []
        while has_next:
            paginated_map_tokens = (
                self.client.Imagery.get_map_tokens(page=page).result()
            )
            map_tokens += [
                MapToken(map_token, self)
                for map_token in paginated_map_tokens.results
            ]
            page = paginated_map_tokens.page + 1
            has_next = paginated_map_tokens.hasNext
        return map_tokens

    @property
    def projects(self):
        """List projects a user has access to

        Returns:
            List[Project]
        """
        has_next = True
        projects = []
        page = 0
        while has_next:
            paginated_projects = self.client.Imagery.get_projects(
                page=page).result()
            has_next = paginated_projects.hasNext
            page = paginated_projects.page + 1
            for project in paginated_projects.results:
                projects.append(Project(project, self))
        return projects

    @property
    def analyses(self):
        """List analyses a user has access to

        Returns:
            List[Analysis]
        """
        has_next = True
        analyses = []
        page = 0
        while has_next:
            paginated_analyses = self.client.Lab.get_tool_runs(page=page).result()
            has_next = paginated_analyses.hasNext
            page = paginated_analyses.page + 1
            for analysis in paginated_analyses.results:
                analyses.append(Analysis(analysis, self))
        return analyses

    @property
    def exports(self):
        """List exports a user has access to

        Returns:
            List[Export]
        """
        has_next = True
        page = 0
        exports = []
        while has_next:
            paginated_exports = self.client.Imagery.get_exports(page=page).result()
            has_next = paginated_exports.hasNext
            page = paginated_exports.page + 1
            for export in paginated_exports.results:
                exports.append(Export(export, self))
        return exports

    def get_datasources(self):
        datasources = []
        for datasource in self.client.Datasources.get_datasources().result().results:
            datasources.append(Datasource(datasource, self))
        return datasources

    def get_datasource_by_id(self, datasource_id):
        return self.client.Datasources.get_datasources_datasourceID(
            datasourceID=datasource_id).result()

    def get_scenes(shape_id=None, bbox=None, datasource=[], maxCloudCover=10, minAcquisitionDatetime=None, maxAcquisitionDatetime=None, **kwargs):
        """
        Get a list of scenes corresponding to datasource type, date, shape and cloudcover.
        
        Common arguments have been included as defaults. For a full list of args
        visit https://doc.radiant.earth/#/scenes/#get

        datasource = list of datasource IDs (each ID is a string)
        shape_id = 'c2fae467-180b-483c-8666-dbbb25181023' (string of shape ID. Use create_shape() to upload shape to platform)
        maxCloudCover = 10 float percentage of cloud cover (0-100, eg 13.26 is valid)
        minAcquisitionDatetime = '2015-01-21T00:00:00.000Z' (string in iso 8601 format)
        maxAcquisitionDatetime = '2017-01-21T00:00:00.000Z' (string in iso 8601 format)
        bbox = '-62.32131958007813,17.472502452750295,-61.60720825195313,17.746070780233786' (string) or shapefile
        """

        if not shape_id or not bbox:
            raise ValueError("Must pass platform shape_id or bbox.")
        if (shape_id and bbox):
            raise ValueError("Can not pass both a shape_id and a bbox.")
        
        params = {}
        
        params['maxCloudCover'] = maxCloudCover
        
        if shape_id:
            params['shape'] = shape_id
        
        if bbox and hasattr(bbox, 'bounds'):
            # if you pass a shapefile
            params['bbox'] = ','.join(str(x) for x in bbox.bounds)
        
        elif bbox and type(bbox) != type(','.join(str(x) for x in bbox)): # NOQA
            # if you pass an array of bounds (similar to shapely's box.bounds)
            params['bbox'] = ','.join(str(x) for x in bbox)
        
        if datasource:
            params['datasource'] = datasource
        
        if minAcquisitionDatetime:
            params['minAcquisitionDatetime'] = minAcquisitionDatetime
            
        if maxAcquisitionDatetime:
            params['maxAcquisitionDatetime'] = maxAcquisitionDatetime
        
        # add kwargs from documentation to params
        # for all arguments to scenes/ endpoint go to
        # https://doc.radiant.earth/#/scenes/#get
        for key, value in kwargs.items():
            params[key] = value
        
            
        return self.client.Imagery.get_scenes(params).result()

    def visualize(scene, zoom_level=0.2, geojson=None, bbox=None):
        """
        Renders footprints of AOI and scene
        """
        
        if geojson:
            # convert aoi to polygon
            aoi_shape = geometry.shape(geojson['geometry'])

        elif bbox:
            # if bbox is str
            if isinstance(bbox, str):
                bbox = [float(x) for x in bbox.split(",")]
            # convert bbox tuple to Polygon
            aoi_shape = geometry.box(*bbox)
        
        else:
            raise ValueError("Must pass either a geojson object or bounding box.")
            
        scene_boundary = scene['dataFootprint']
        scene_boundary_shape = geometry.shape(scene_boundary)

        # grab center from aoi polygon/multipolygon
        center = aoi_shape.centroid.coords[0]
        albers = cartopy.crs.AlbersEqualArea(central_latitude=center[1], central_longitude=center[0])
        lonlat_crs = cartopy.crs.PlateCarree()

        fig = plt.figure(figsize=(6, 8))

        # specify projection of the map
        ax = plt.subplot(projection=albers)
        
        # add scene and aoi geometries to plot
        ax.add_geometries([aoi_shape], lonlat_crs, alpha=0.5, color='blue')
        ax.add_geometries([scene_boundary_shape], lonlat_crs, alpha=0.1, color='green')
        
        
        # create a combined polygon to ensure both aoi and scene footprints are displayed 
        aoi_polygon_objects = [aoi_shape]
        aoi_polygon_objects.extend([scene_boundary_shape])
        combined_polygon = cascaded_union(aoi_polygon_objects)
        
        # set the plot extent to the edges of the combined_polygon
        combined_bbox = combined_polygon.bounds
        combined_extent = (combined_bbox[0], combined_bbox[2], combined_bbox[1], combined_bbox[3])

        # zoom out slightly beyond edge of aoi and scene polygons
        zoom = (-zoom_level, zoom_level, -zoom_level, zoom_level)
        combined_extent_zoom = [sum(x) for x in zip(combined_extent, zoom)]

        # apply extent to actual plot
        ax.set_extent(combined_extent_zoom, crs=lonlat_crs)
        ax.gridlines(crs=lonlat_crs)

        datasource_name = scene['datasource']['name']
        cloud_cover = get_cloud_cover(scene)
        acquisition_date = get_timestamp(scene)
        scene_id = scene['id']

        print(datasource_name)
        print("Acquisition date = {}".format(acquisition_date))
        print("Cloud cover percentage = {}".format(cloud_cover))
        print("Scene ID = {}".format(scene_id))

        plt.title('{}'.format(scene_id))
        plt.show()


    def get_project_config(self, project_ids, annotations_uris=None):
        """Get data needed to create project config file for prep_train_data

        The prep_train_data script requires a project config files which
        lists the images and annotation URIs associated with each project
        that will be used to generate training data. If the annotation_uris
        are not specified, an annotation file for each project will be
        generated and saved to S3.

        Args:
            project_ids: list of project ids to make training data from
            annotations_uris: optional list of corresponding annotation URIs

        Returns:
            Object of form [{'images': [...], 'annotations':...}, ...]
        """
        project_configs = []
        for project_ind, project_id in enumerate(project_ids):
            proj = Project(
                self.client.Imagery.get_projects_uuid(uuid=project_id).result(),
                self)

            if annotations_uris is None:
                annotations_uri = os.path.join(
                    RV_TEMP_URI, 'annotations', '{}.json'.format(uuid.uuid4()))
                proj.save_annotations_json(annotations_uri)
            else:
                annotations_uri = annotations_uris[project_ind]

            image_uris = proj.get_image_source_uris()
            project_configs.append({
                'id': project_id,
                'images': image_uris,
                'annotations': annotations_uri
            })

        return project_configs

    def save_project_config(self, project_ids, output_uri,
                            annotations_uris=None):
        """Save project config file.

        This file is needed by Raster Vision to prepare training data, make
        predictions, and evaluate predictions.

        Args:
            project_ids: list of project ids to make training data from
            output_path: where to write the project config file
            annotations_uris: optional list of corresponding annotation URIs
        """
        project_config = self.get_project_config(
            project_ids, annotations_uris)
        project_config_str = json.dumps(
            project_config, sort_keys=True, indent=4)

        str_to_file(project_config_str, output_uri)
