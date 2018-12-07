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

from shapely import geometry
from shapely.ops import cascaded_union
from matplotlib import pyplot as plt
import cartopy

import datetime

from .utils import get_all_paginated

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

    def projects(self, ownershipType="owned", dict_out=True):
        """List projects a user has access to. Defaults to 
        ownershipType="owned" but can also pass in "shared" or
        "inherited" ownershipTypes. if dict_out==True, returns 
        a dictionary, otherwise returns a list of Project objects.

        Returns:
            
            by default-
            {proj_id:Project}
            
            optionally-
            List[Project]
        """
        has_next = True
        projects = []
        page = 0
        while has_next:
            paginated_projects = self.client.Imagery.get_projects(
                ownershipType=ownershipType, page=page).result()
            has_next = paginated_projects.hasNext
            page = paginated_projects.page + 1
            for project in paginated_projects.results:
                projects.append(Project(project, self))
        
        if dict_out:
            return {project.id:project for project in projects}
        
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
        """
        Returns dict of all datasources accessible by your account
        """

        datasources_list = []
        
        # instantiate Datasource objects
        for datasource in self.client.Datasources.get_datasources().result().results:
            datasources_list.append(Datasource(datasource, self))
        
        # put in dictionary for easy access
        ds_dict = {x.name:x for x in datasources_list}
        
        return ds_dict

    def get_datasource_by_id(self, datasource_id):
        """
        Get details of datasource by id lookup
        """

        return self.client.Datasources.get_datasources_datasourceID(
            datasourceID=datasource_id).result()
    
    def load_geojson(self, geojson_filepath):
        """
        Open a geojson and return a geojson object.
        """
        with open(geojson_filepath) as f:
            geojson = json.load(f)

        return geojson
    
    def create_shape(self, geojson_dict):
        """
        Takes a dict of names and GeoJSON objects (geojson_dict), uploads to platform.
        Returns similar dict with names and platform IDs.
        
        Input:
        
        geojson_dict = {'name_of_geojson': geojson}
        
        Output:

        {'name_of_geojson': platform-generated ID}.
                
        *Note* Each geojson must be a single Feature (not a GeoJSON Feature Collection).
        A Feature is a dictionary with the keys, 'type', 'geometry', and 'properties'.
        
        A good resource about Features, FeatureCollections and GeoJSONs:
        https://macwright.org/2015/03/23/geojson-second-bite.html
        """
        
        shapes = []
        
        feature_keys = {"type", "geometry", "properties"}

        for key, val in geojson_dict.items():
            
            # quick and dirty data validation
            try:
                message = "Input geodata '{}' incorrect, confirm geodata is a GeoJSON Feature.".format(key)
                if geojson_dict[key].keys() != feature_keys:
                    raise ValueError(message)
            except AttributeError as e:
                raise Exception(message).with_traceback(e.__traceback__)
            
            # purge existing geojson 'properties' for platform upload compatability
            geojson_dict[key]['properties'] = {'name':key}
            
            # add shape to list for upload
            shapes.append(geojson_dict[key])
        
        # create GeoJson FeatureCollection
        fc = {"type": "FeatureCollection", "features": shapes}
        
        results = self.client.Imagery.post_shapes(shapes=fc).future.result().json()
        return {shape['properties']['name']: shape['id'] for shape in results}
         
    
    def get_scenes(self, shape_id=None, bbox=None, datasource=[], maxCloudCover=10, minAcquisitionDatetime=None, maxAcquisitionDatetime=None, **kwargs):
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

        if not shape_id and not bbox:
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
        
        return self.client.Imagery.get_scenes(**params).result()
    
    def polygon_from_shape_id(self, shape_id):
        """
        Build a polygon from a platform shape_id.
        """
        geojson = self.client.Imagery.get_shapes_shapeID(shapeID=shape_id).future.result().json()
        shape = geometry.shape(geojson['geometry'])
        
        return shape

    def coordinates_from_shape_id(self, shapeID):
        """
        Returns Features list of coordinates for one or more GeoJSON features
        """
        def convert_to_lists(t):
            return list(map(convert_to_lists, t)) if isinstance(t, (list, tuple)) else t
        
        return convert_to_lists(self.polygon_from_shape_id(shapeID).__geo_interface__['coordinates'])

    def get_shapes(self):
        """
        Returns dict of {shapeID:name} of all shapes accessible to user.
        """
        def get_page(page):
            return self.client.Imagery.get_shapes(page=page).result()
        
        all_results = get_all_paginated(get_page, list_field='features')
        
        return {shape.id:shape.properties['name'] for shape in all_results}

    def get_cloud_cover(self, scene):
        """
        Parses either Scene object or scene JSON metadata and returns cloudcover.
        
        Works with L8 and S2 datatypes.
        """
        
        # convert scene objects to dict so works with two datatypes
        if not isinstance(scene, dict):
            scene = scene.__dict__
            scene = scene['_Model__dict']
        
        cloudCover = None
        scene_id = scene['datasource']['id']
        
        if scene_id == '697a0b91-b7a8-446e-842c-97cda155554d': # Lansat 8
            # L8 also has 'CLOUD_COVER_LAND', a computation of cloud cover 
            # over land-classified pixels
            cloudCover = scene['sceneMetadata']['cloudCover']
            
        elif scene_id == '4a50cb75-815d-4fe5-8bc1-144729ce5b42': # Sentinel-2
            # from S2 docs: cloudyPixelPercentage = 'Percentage of cloud coverage'
            cloudCover = scene['sceneMetadata']['cloudyPixelPercentage']
        
        return cloudCover

    def convert_date_isoformat(self, datestring):
        """"
        Convert datestrings  of either L8 or MODIS to Iso 8601 timestamp format
        """
        
        # MODIS/Terra and MODIS/Aqua
        if len(datestring) == 7:
            d = datetime.datetime.strptime(datestring, '%Y%j')
        
        # Landsat 8 format 'yyyy-mm-dd'
        elif len(datestring) == 10:
            d = datetime.datetime.strptime(datestring, "%Y-%m-%d")
        
        d = d.isoformat()+".000Z"
        
        return d

    def get_timestamp(self, scene):
        """
        Parses either Scene object or scene JSON metadata and returns timestamp
        """
        
        # convert scene objects to dict so works with two datatypes
        if not isinstance(scene, dict):
            scene = scene.__dict__
            scene = scene['_Model__dict']

        timestamp = None
        
        if scene['datasource']['id'] == '697a0b91-b7a8-446e-842c-97cda155554d': # Lansat 8
            timestamp = scene['sceneMetadata']['acquisitionDate']
            # L8 needs to reformat timestamp to match others
            timestamp = self.convert_date_isoformat(timestamp)
       
        # S2 aquisition date is already in correct time format
        elif scene['datasource']['id'] == '4a50cb75-815d-4fe5-8bc1-144729ce5b42': # Sentinel-2
            timestamp = scene['sceneMetadata']['timeStamp']
        
        elif scene['datasource']['id'] == 'a11b768b-d869-476e-a1ed-0ac3205ed761': # MODIS/Terra
            timestamp = scene['name'].split(".")[1][1:] # str in julian date
            timestamp = self.convert_date_isoformat(timestamp) # standardize
        
        elif scene['datasource']['id'] == '55735945-9da5-47c3-8ae4-572b5e11205b': #MODIS/Aqua
            timestamp = scene['name'].split(".")[1][1:] # str in julian date
            timestamp = self.convert_date_isoformat(timestamp) # standardize

        return timestamp

    def fill_aoi(self, results, aoi_polygon, datasource_id):
        """
        Takes a list of scene objects (from get_scenes()) and returns 
        an ordered list of scene ids from most recent time (in results query) 
        that cover aoi. First position in list represents most recent scene. 
        """
        
        scene_boundaries = []
        scene_ids = []
        
        for scene in results:
            
            # check datasource
            if scene.datasource.id != datasource_id:
                raise ValueError(
                    "Scene datasource type '{}' doesn't match specified datasource type '{}'.".format(
                        scene.datasource.id, datasource_id))
                    
            # convert scene boundary to polygon
            scene_boundary = scene.dataFootprint
            scene_boundary_shape = geometry.shape(scene_boundary.__dict__['_Model__dict'])
            
            # add scene polygon to list of scene polygons
            scene_boundaries.append(scene_boundary_shape)
            # add scene id to list of scene ids
            scene_ids.append(scene.id)
            # merge all scene polygon shapes into 1 larger polygon
            aoi_coverage = cascaded_union(scene_boundaries)
            
            if aoi_coverage.contains(aoi_polygon):
                
                return scene_ids
        
        # if aoi cannot be covered by scenes    
        return "Insufficient imagery available."

    def visualize(self, scene, zoom_level=0.2, aoi_polygon=None, bbox=None):
        """
        Renders footprints of AOI and scene
        """
        
        if aoi_polygon:
            aoi_shape = aoi_polygon

        elif bbox:
            # if bbox is str
            if isinstance(bbox, str):
                bbox = [float(x) for x in bbox.split(",")]
            # convert bbox tuple to Polygon
            aoi_shape = geometry.box(*bbox)
        
        else:
            raise ValueError("Must pass either a polygon object or bounding box.")
        
        scene_boundary = scene.dataFootprint
        scene_boundary_shape = geometry.shape(scene_boundary.__dict__['_Model__dict'])        
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
        cloud_cover = self.get_cloud_cover(scene)
        acquisition_date = self.get_timestamp(scene)
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
