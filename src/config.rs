
use std::env::args;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Role {
    Slave,
    Master
}

impl ToString for Role {
    fn to_string(&self) -> String {
        match *self {
            Role::Slave => String::from("slave"),
            Role::Master => String::from("master"),
        }
    }
}


#[derive(Debug, Clone)]
pub struct InstanceConfig {
    port: u32,
    role: Role,
    replicaof: Option<String>
}

impl Default for InstanceConfig {
    fn default() -> Self {
        InstanceConfig{port: 6379, role: Role::Master, replicaof: None}
    }
}

impl InstanceConfig {
    pub fn port(&self) -> u32 {
        self.port
    }
    pub fn role(&self) -> Role {
        self.role.clone()
    }

    pub fn from_command_args() -> Self {
        let mut output = InstanceConfig::default();
        let args = args().into_iter().collect::<Vec<_>>();
        args.iter().enumerate().for_each( |(i, arg)| {
            match arg.as_str() {
                "--port" => output.port = args[i + 1].parse::<u32>().unwrap(),
                "--replicaof" => {
                        output.role = Role::Slave;
                        output.replicaof = Some(args[i + 1].clone())
                }
                _ => {}
            }
        });

        output
    }
}
