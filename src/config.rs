use std::env::args;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Role {
    Slave,
    Master,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a_str = match *self {
            Role::Slave => "slave",
            Role::Master => "master",
        };
        write!(f, "{a_str}")
    }
}

#[derive(Debug, Clone)]
pub struct InstanceConfig {
    port: u32,
    role: Role,
    replicaof: Option<String>,
}

impl Default for InstanceConfig {
    fn default() -> Self {
        InstanceConfig {
            port: 6379,
            role: Role::Master,
            replicaof: None,
        }
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
        let args = args().collect::<Vec<_>>();
        args.iter()
            .enumerate()
            .for_each(|(i, arg)| match arg.as_str() {
                "--port" => output.port = args[i + 1].parse::<u32>().unwrap(),
                "--replicaof" => {
                    output.role = Role::Slave;
                    output.replicaof = Some(args[i + 1].clone())
                }
                _ => {}
            });

        output
    }
}
