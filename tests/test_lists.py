import unittest
import os 

from gwf.parser import parse
testdir = os.path.dirname(os.path.realpath(__file__))

def _fname(workflow, fname):
    return os.path.normpath(os.path.join(workflow.working_dir, fname))



class ListTester(unittest.TestCase):

    def setUp(self):
        self.workflow = parse(os.path.join(testdir,'lists.gwf'))

    def test_lists(self):
        self.assertItemsEqual(['singleton','list1','list2','list3','concat'],
                              self.workflow.lists)
                              
        singleton = self.workflow.lists['singleton']
        self.assertEqual('singleton', singleton.name)
        self.assertItemsEqual(['x'], singleton.elements)
        
        list1 = self.workflow.lists['list1']
        self.assertEqual('list1', list1.name)
        self.assertItemsEqual(['elm1','elm2','elm3'], list1.elements)
        
        list2 = self.workflow.lists['list2']
        self.assertEqual('list2', list2.name)
        self.assertItemsEqual(['x','y','z'], list2.elements)
        
    def test_targets(self):
        self.assertItemsEqual(['singleton',
                                'one_elm1','one_elm2','one_elm3',
                                'two_single_a','two_single_b','two_single_c',
                                'two_elm1_x','two_elm2_y','two_elm3_z'],
                              self.workflow.targets)
                              
        one = self.workflow.targets['one_elm1']
        self.assertItemsEqual([_fname(self.workflow,'elm1')],one.input)
        self.assertItemsEqual([_fname(self.workflow,'elm1.out')],one.output)

        one = self.workflow.targets['one_elm2']
        self.assertItemsEqual([_fname(self.workflow,'elm2')],one.input)
        self.assertItemsEqual([_fname(self.workflow,'elm2.out')],one.output)

        one = self.workflow.targets['one_elm3']
        self.assertItemsEqual([_fname(self.workflow,'elm3')],one.input)
        self.assertItemsEqual([_fname(self.workflow,'elm3.out')],one.output)

        
        two = self.workflow.targets['two_elm1_x']
        self.assertItemsEqual([_fname(self.workflow,'elm1')],two.input)
        self.assertItemsEqual([_fname(self.workflow,'x')],two.output)
        
        two = self.workflow.targets['two_elm2_y']
        self.assertItemsEqual([_fname(self.workflow,'elm2')],two.input)
        self.assertItemsEqual([_fname(self.workflow,'y')],two.output)
        
        two = self.workflow.targets['two_elm3_z']
        self.assertItemsEqual([_fname(self.workflow,'elm3')],two.input)
        self.assertItemsEqual([_fname(self.workflow,'z')],two.output)
        
    def test_concat(self):
        concat = self.workflow.lists['concat']
        self.assertEqual(concat.elements, ['x','y','z','a','b','c'])