"""A Project is a collection of zero or more scenes"""
import copy
import json
import uuid
from datetime import date, datetime

import requests

from .export import Export
from .map_token import MapToken
from .. import NOTEBOOK_SUPPORT
from ..aws.s3 import file_to_str, str_to_file
from ..decorators import check_notebook
from ..exceptions import GatewayTimeoutException
from ..utils import get_all_paginated

if NOTEBOOK_SUPPORT:
    from ipyleaflet import (
        Map,
#         SideBySideControl,
        TileLayer,
    )


class Project(object):
    """A Radiant Earth project"""

    TILE_PATH_TEMPLATE = '/tiles/{id}/{{z}}/{{x}}/{{y}}/'
    EXPORT_TEMPLATE = '/tiles/{project}/export/'

    def __repr__(self):
        return '<Project - {}>'.format(self.name)

    def __init__(self, project, api):
        """Instantiate a new Project

        Args:
            project (Project): generated Project objects from specification
            api (API): api used to make requests on behalf of a project
        """
        self._project = project
        self.api = api

        # A few things we care about
        self.name = project.name
        self.id = project.id

    @classmethod
    def create(
            cls, api, name, description="", visibility="PRIVATE", 
            tileVisibility="PRIVATE", isAOIProject=False, tags=[]):
        """
        Creates a new project on the platform.
        
        Args:

        api is the api object instantiated with your refresh token

        name is a string defining the project's name
        
        description is a string containing a breif description of your project
        
        visibility can be PRIVATE (only accessible to you) or PUBLIC 
        (accessible to everyone on the platform)
        
        tileVisibility can be PRIVATE (only accessible to you or others whom 
        you share a mapToken with) or PUBLIC (accessible to everyone)
        
        tags is an array of strings 
        
        isAOIProject is a boolean variables (True or False) that specifies 
        if a project is Standard (False) or AOI (True).
        AOI Project is used to actively monitor if a new scene is available 
        over your area of interest. New scenes will be        
        
        Returns:
            Radiant Earth Project object
        """

        body = {"name": name, 
                  "description": description, 
                  "visibility": visibility, 
                  "tileVisibility": tileVisibility, 
                  "tags": tags, 
                  "isAOIProject": isAOIProject 
                 }
        
        project = api.client.Imagery.post_projects(project=body).result()
 
        return Project(project, api)

    def add_scenes(self, sceneIDs=[]):
        """
        Add unordered list of scenes to a project.
        Returns integer of number of new scenes being ingested into project.
        """
        return self.api.client.Imagery.post_projects_projectID_scenes(
                projectID=self.id, scenes=sceneIDs).future.result().json()    
    
    def set_scene_order(self, sceneIDs):
        """
        Set the order of scenes in a project. The first scene id in sceneIDs will be
        on the very top layer, the last will be on the bottom.
        
        Args:
            sceneIDs (list): a list of platform defined scene ids already in the project.
            (functionality still works even if one or more of the scenes is still being
            ingested.)
            
            ex:
            
            sceneIDs = ['cf25a31c-364a-4e6b-9c0f-e92a8732e0a3','da841d25-9fd5-476b-ae10-a029d0e25a41']
            
        Returns:
            Nothing, but will raise a KeyError or ValueError if sceneIDs don't
            1:1 match actual scenes in project.
            
            Raises an http error if a non-http 2XX status code is returned.
        """
        proj_scenes = {scene.id for scene in self.get_scenes()}
        
        # validation
        for scene in sceneIDs:
            proj_scenes.remove(scene)
        if proj_scenes:
            raise ValueError(
                    "Input sceneIDs did not match actual project sceneIDs")
        
        r = self.api.client.Imagery.put_projects_projectID_order(projectID=self.id, sceneIDs=sceneIDs).future.result()
        
        # throw error if not a 2XX status code
        r.raise_for_status()

    def add_ordered_scenes(self, sceneIDs):
        """
        Adds list of scenes to a project and then sets the
        order of the scenes to the same as the order in
        the list.
        
        For more information see add_scenes() and
        set_scene_order().
        """
        scenes_added = self.add_scenes(sceneIDs=sceneIDs)
        print("{} scenes added to project.".format(scenes_added))
        self.set_scene_order(sceneIDs=sceneIDs)

    def get_center(self):
        """Get the center of this project's extent"""
        coords = self._project.extent.get('coordinates')
        if not coords:
            raise ValueError(
                'Project must have coordinates to calculate a center'
            )
        x_min = min(
            coord[0] + (360 if coord[0] < 0 else 0) for coord in coords[0]
        )
        x_max = max(
            coord[0] + (360 if coord[0] < 0 else 0) for coord in coords[0]
        )
        y_min = min(coord[1] for coord in coords[0])
        y_max = max(coord[1] for coord in coords[0])
        center = [(y_min + y_max) / 2., (x_min + x_max) / 2.]
        if center[0] > 180:
            center[0] = center[0] - 360
        return tuple(center)

    def get_map_token(self):
        """Returns the map token for this project

        Returns:
            str
        """

        resp = (
            self.api.client.Imagery.get_map_tokens(project=self.id).result()
        )
        if resp.results:
            return MapToken(resp.results[0], self.api)

    def get_thumbnail(self, bbox, zoom, export_format, raw):
        headers = self.api.http.session.headers.copy()
        headers['Accept'] = 'image/{}'.format(
            export_format
            if export_format.lower() in ['png', 'tiff']
            else 'png'
        )
        export_path = self.EXPORT_TEMPLATE.format(project=self.id)
        request_path = '{scheme}://{host}{export_path}'.format(
            scheme=self.api.scheme, host=self.api.tile_host,
            export_path=export_path
        )

        response = requests.get(
            request_path,
            params={
                'bbox': bbox,
                'zoom': zoom,
                'token': self.api.api_token,
                'colorCorrect': 'false' if raw else 'true'
            },
            headers=headers
        )
        if response.status_code == requests.codes.gateway_timeout:
            raise GatewayTimeoutException(
                'The export request timed out. '
                'Try decreasing the zoom level or using a smaller bounding box.'
            )
        response.raise_for_status()
        return response

    def create_export(self, resolution, coordinates):
        """
        Create an export process. Returns an exportID.
        By default requires coordinates generated from 
        api.coordinates_from_shape_id(shapeID).
        
        If coordinates not passed in, generates an export
        covering all scenes in project.
        
        Resolution is the zoomed level applied to all bands of data.
        
        For more information on resolution:
        
        https://wiki.openstreetmap.org/wiki/Zoom_levels
        
        Detailed information about the resolution/band of Landsat:
        
        https://landsat.usgs.gov/what-are-band-designations-landsat-satellites
        
        Detailed information about the resolution/band of Sentinel-2:
        
        https://earth.esa.int/web/sentinel/user-guides/sentinel-2-msi/resolutions/spatial
        
        and
        
        https://www.gdal.org/frmt_sentinel2.html
        
        *Note* As of 12/06/2018 exporting unprojected (Web Mercator)
        and native pixel resolution (unzoomed) is not possible. We are 
        working towards enabling this functionality in future releases.
        
        """

        # to cut an export to a shape
        if coordinates:
            
            # Confirm/map coordinates are/to MultiPolygon format (depth = 4)
            depth = lambda L: isinstance(L, list) and max(map(depth, L)) + 1
            
            if depth(coordinates) == 3:
                coordinates = [coordinates]
                
            if depth(coordinates) != 4:
                raise ValueError("Coordinates do have correct dimmension")
            
            export = {
                'projectId': self.id,
                # for an analysis
                'toolRunId': None,
                'exportStatus': 'NOTEXPORTED',
                'exportType': 'S3',
                'visibility':'PRIVATE',
                'exportOptions': { 'resolution': resolution,
                                  'crop': False,
                                  'raw': False, 
                                  'mask': {
                                      'type': 'MultiPolygon',
                                      'coordinates': coordinates
                                  }
                            }
                        }
            
        # to generate an export covering all scenes in project
        else:
            export = {
            'projectId': self.id,
            # for an analysis
            'toolRunId': None,
            'exportStatus': 'NOTEXPORTED',
            'exportType': 'S3',
            'visibility':'PRIVATE',
            'exportOptions': { 'resolution': resolution,
                              'crop': False,
                              'raw': False, 
                              'mask': None
                        }
                    }
        
        return self.api.client.Imagery.post_exports(Export=export).future.result().json()['id']

    def export_from_shape_id(self, shapeID, resolution=10):
        """
        Creates an export from a shapeId. For more information
        on resolution, see docstring for create_export()
        """
        coordinates = self.api.coordinates_from_shape_id(shapeID)
        return self.create_export(resolution, coordinates)

    def get_export_status(self, exportID):
        """
        Returns export status. Possible values are:
        "NOTEXPORTED" - Export created.
        "TOBEEXPORTED" - Export queued. 
        "EXPORTING" - Export in progress.
        "EXPORTED" - Export complete, available at url.
        "FAILED" - Export failed.
        """
        r = (self.api.client.Imagery.get_exports_exportID(exportID=exportID)
                 .future
                 .result()
                 .json()['exportStatus']
            )
        
        return r


    def geotiff(self, bbox, zoom=10, raw=False):
        """Download this project as a geotiff

        The returned string is the raw bytes of the associated geotiff.

        Args:
            bbox (str): Bounding box (formatted as 'x1,y1,x2,y2') for the download
            zoom (int): zoom level for the export

        Returns:
            str
        """

        return self.get_thumbnail(bbox, zoom, 'tiff', raw).content

    def png(self, bbox, zoom=10, raw=False):
        """Download this project as a png

        The returned string is the raw bytes of the associated png.

        Args:
            bbox (str): Bounding box (formatted as 'x1,y1,x2,y2') for the download
            zoom (int): zoom level for the export

        Returns
            str
        """

        return self.get_thumbnail(bbox, zoom, 'png', raw).content

    def tms(self):
        """Return a TMS URL for a project"""

        tile_path = self.TILE_PATH_TEMPLATE.format(id=self.id)
        return '{scheme}://{host}{tile_path}?token={token}'.format(
            scheme=self.api.scheme, host=self.api.tile_host,
            tile_path=tile_path, token=self.api.api_token
        )

    def post_annotations(self, annotations_uri):
        annotations = json.loads(file_to_str(annotations_uri))
        # Convert RV annotations to RE format.
        rf_annotations = copy.deepcopy(annotations)
        for feature in rf_annotations['features']:
            properties = feature['properties']
            feature['properties'] = {
                'label': properties['class_name'],
                'description': '',
                'machineGenerated': True,
                'confidence': properties['score']
            }

        self.api.client.Imagery.post_projects_uuid_annotations(
            uuid=self.id, annotations=rf_annotations).future.result()

    def get_annotations(self):
        def get_page(page):
            return self.api.client.Imagery.get_projects_uuid_annotations(
                uuid=self.id, page=page).result()

        return get_all_paginated(get_page, list_field='features')

    def save_annotations_json(self, output_uri):
        features = self.get_annotations()
        geojson = {'features': [feature._as_dict() for feature in features]}

        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code."""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError('Type {} not serializable'.format(str(type(obj))))

        geojson_str = json.dumps(geojson, default=json_serial)
        str_to_file(geojson_str, output_uri)

    def get_scenes(self):
        def get_page(page):
            return self.api.client.Imagery.get_projects_projectID_scenes(
                projectID=self.id, page=page).result()

        return get_all_paginated(get_page)

    def get_ordered_scene_ids(self):
        """
        Returns dict of {order:sceneID} for all scenes in project.
        Order is 0-indexed, with 0 representing the top-most-layer
        of the composite.
        """
        return {scene.sceneOrder:scene.id for scene in self.get_scenes()}

    def get_scenes_ingest_status(self):
        return {scene.id:scene.statusFields.ingestStatus for scene in self.get_scenes()}
    
    def get_image_source_uris(self):
        """Return sourceUris of images for with this project sorted by z-index."""
        source_uris = []

        scenes = self.get_scenes()
        ordered_scene_ids = self.get_ordered_scene_ids()

        id_to_scene = {}
        for scene in scenes:
            id_to_scene[scene.id] = scene
        sorted_scenes = [id_to_scene[scene_id] for scene_id in ordered_scene_ids]

        for scene in sorted_scenes:
            for image in scene.images:
                source_uris.append(image.sourceUri)

        return source_uris

    def start_predict_job(self, rv_batch_client, inference_graph_uri,
                          label_map_uri, predictions_uri,
                          channel_order=[0, 1, 2]):
        """Start a Batch job to perform object detection on this project.

        Args:
            rv_batch_client: instance of RasterVisionBatchClient used to start
                Batch jobs
            inference_graph_uri (str): file with exported object detection
                model file
            label_map_uri (str): file with mapping from class id to display name
            predictions_uri (str): GeoJSON file output by the prediction job
            channel_order (list of int)
        Returns:
            job_id (str): job_id of job started on Batch
        """
        source_uris = self.get_image_source_uris()
        source_uris_str = ' '.join(source_uris)
        channel_order = ' '.join([str(channel) for channel in channel_order])
        # Add uuid to job_name because it has to be unique.
        job_name = 'predict_project_{}_{}'.format(self.id, uuid.uuid1())
        command = 'python -m rv.detection.run predict --channel-order {} {} {} {} {}'.format(  # noqa
            channel_order, inference_graph_uri, label_map_uri, source_uris_str,
            predictions_uri)
        job_id = rv_batch_client.start_raster_vision_job(job_name, command)

        return job_id

    @check_notebook
    def add_to(self, leaflet_map):
        """Add this project to a leaflet map

        Args:
            leaflet_map (Map): map to add this layer to
        """

        leaflet_map.add_layer(self.get_layer())

    @check_notebook
    def compare(self, other, leaflet_map):
        """Add a slider to compare two projects

        This project determines the map center.

        Args:
            other (Project): the project to compare with this project
            leaflet_map (Map): map to add the slider to
        """

        control = SideBySideControl(
            leftLayer=self.get_layer(), rightLayer=other.get_layer()
        )
        leaflet_map.add_control(control)

    @check_notebook
    def get_layer(self):
        """Returns a TileLayer for display using ipyleaflet"""
        return TileLayer(url=self.tms())

    @check_notebook
    def get_map(self, **kwargs):
        """Return an ipyleaflet map centered on this project's center

        Args:
            **kwargs: additional arguments to pass to Map initializations
        """
        default_url = (
            'https://cartodb-basemaps-{s}.global.ssl.fastly.net/'
            'light_all/{z}/{x}/{y}.png'
        )
        return Map(
            default_tiles=TileLayer(url=kwargs.get('url', default_url)),
            center=self.get_center(),
            scroll_wheel_zoom=kwargs.get('scroll_wheel_zoom', True),
            **kwargs
        )
