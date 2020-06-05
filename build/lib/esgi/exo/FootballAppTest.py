import random
import unittest
from esgi.exo.FootballApp import FootballApp

class FootballAppTest(unittest.TestCase):

    """Tests de la classe FootballApp."""

    def setUp(self):
        """Initialisation des tests."""
        self.footballApp = FootballApp()

    def testCreateSparkSession(self):
        """Test le fonctionnement de la fonction 'createSparkSession'."""
        sparkSession = self.footballApp.createSparkSession()
        self.assertIsNotNone(sparkSession)