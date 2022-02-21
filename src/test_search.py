import unittest
import search as srch


class TestSearch(unittest.TestCase):


    """
    Test if the campaigns endpoint works
    """
    def test_get_campaigns(self):
        srh = srch.Search('localhost', 'bidnamic', 'camer', 'camer')
        search_term = 'sorbothane'
        res = srh.get_campaigns(search_term)
        self.assertEqual(res, ('sorbothane', 'sorbothane', 1.9, 22.0, 11.3))


    """
    Test if the adgroups endpoint works
    """
    def test_get_adgroups(self):
        srh = srch.Search('localhost', 'bidnamic', 'camer', 'camer')
        search_term = 'black'
        res = srh.get_adgroups(search_term)
        self.assertEqual(res, ('Shift - Shopping - GB - adidas - HIGH - beryllium-black-august-social - 66102e4df9334f93a8078e2c0037a15a', 'black', 2.2, 51.0, 23.3))


    """
    Test if the search_terms endpoint works
    """
    def test_get_search_terms(self):
        srh = srch.Search('localhost', 'bidnamic', 'camer', 'camer')
        search_term = 'black'
        res = srh.get_search_terms(search_term)
        self.assertEqual(res, ['black', 2.2, 51.0, 23.3])


if __name__ == "__main__":
    unittest.main()
