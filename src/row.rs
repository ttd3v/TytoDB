use std::collections::HashMap;
use crate::alba_types::AlbaTypes;


#[derive(Clone,Debug)]
pub struct Row{
    pub data : HashMap<String,AlbaTypes>,
    pub metadata : HashMap<String,AlbaTypes>
}
impl Row{
    fn new(self,data : HashMap<String, AlbaTypes>, metadata : HashMap<String,AlbaTypes>) -> Self{
        Row { data,metadata }
    }
}